package example;

import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Errors {
    private static final ThreadLocal<ErrorStatus> currentErrorStatus = new ThreadLocal<>();

    private static final Logger logger = Logger.getLogger(Logger.class.getName());

    public static void setCurrentErrorStatus(ErrorStatus error) {
        currentErrorStatus.set(error);
    }

    /**
     * ServerInterceptor which includes additional error information in error response metadata. Error information
     * is retrieved from a thread local which must be set before StreamObserver.onError() is called from the service
     * method. This also assumes that StreamObserver.onError() and ServerCall.Listener.close() are invoked in the
     * same thread for the thread local passing method to work.
     */
    public static ServerInterceptor serverInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, ResT> ServerCall.Listener<ReqT> interceptCall(
                String method, ServerCall<ResT> call, Metadata.Headers headers, ServerCallHandler<ReqT, ResT> next) {
                return next.startCall(method, new ForwardingServerCall.SimpleForwardingServerCall<ResT>(call) {
                    @Override
                    public void close(Status status, Metadata.Trailers trailers) {
                        if (!status.isOk()) {
                            ErrorStatus errorStatus = currentErrorStatus.get();
                            if (errorStatus != null) {
                                currentErrorStatus.remove();
                                errorStatus.addHeaders(trailers);
                            }
                        }
                        super.close(status, trailers);
                    }
                }, headers);
            }
        };
    }

    /**
     * ClientInterceptor which captures additional error information from error response metadata if present.
     */
    public static ClientInterceptor clientInterceptor(Consumer<ErrorStatus> statusConsumer) {
        return new ClientInterceptor() {
            @Override
            public <ReqT, ResT> ClientCall<ReqT, ResT> interceptCall(MethodDescriptor<ReqT, ResT> method,
                                                                     CallOptions callOptions, Channel next) {
                return capturingCall(next.newCall(method, callOptions), statusConsumer);
            }
        };
    }

    /**
     * Forwarding call which captures error information from error response metadata if present.
     */
    public static <ReqT, ResT> ClientCall<ReqT, ResT> capturingCall(ClientCall<ReqT, ResT> delegate,
                                                                    Consumer<ErrorStatus> statusConsumer) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, ResT>(delegate) {
            @Override
            public void start(Listener<ResT> listener, Metadata.Headers headers) {
                super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<ResT>(listener) {
                    @Override
                    public void onClose(Status status, Metadata.Trailers trailers) {
                        if (!status.isOk()) {
                            ErrorStatus.fromMetadata(trailers).ifPresent(statusConsumer::accept);
                        }
                        super.onClose(status, trailers);
                    }
                }, headers);
            }
        };
    }

    /**
     * Forwarding future which rethrows StatusRuntimeException as GrpcException including additional error
     * information from error response metadata if present.
     */
    public static <V> ListenableFuture<V> futureWithStatus(Supplier<ErrorStatus> statusSupplier,
                                                           ListenableFuture<V> delegate) {
        return new ForwardingListenableFuture.SimpleForwardingListenableFuture<V>(delegate) {
            @Override
            public V get() throws InterruptedException, ExecutionException {
                try {
                    return super.get();
                } catch (ExecutionException e) {
                    throw new ExecutionException(e.getMessage(),
                        ErrorException.fromThrowable(e.getCause()).withErrorStatus(statusSupplier.get()));
                }
            }

            @Override
            public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                try {
                    return super.get(timeout, unit);
                } catch (ExecutionException e) {
                    throw new ExecutionException(e.getMessage(),
                        ErrorException.fromThrowable(e.getCause()).withErrorStatus(statusSupplier.get()));
                }
            }
        };
    }

    /**
     * Forwarding StreamObserver which calls StreamObserver.onError with an instance of GrpcException including
     * additional error information from response metadata.
     */
    public static <V> StreamObserver<V> streamObserverWithStatus(Supplier<ErrorStatus> statusSupplier,
                                                                 StreamObserver<V> delegate) {
        return new StreamObserver<V>() {
            @Override
            public void onValue(V v) {
                delegate.onValue(v);
            }

            @Override
            public void onError(Throwable throwable) {
                delegate.onError(ErrorException.fromThrowable(throwable).withErrorStatus(statusSupplier.get()));
            }

            @Override
            public void onCompleted() {
                delegate.onCompleted();
            }
        };
    }

    /**
     * Used in service methods to pass appropriate exception to responseObserver with additonal error information
     * to set on response metadata. Also logs errors that aren't due to client errors (e.g. invalid argument, etc).
     */
    public static <V> void handleError(Throwable t, StreamObserver<V> responseObserver) {
        ErrorException e = ErrorException.fromThrowable(t);
        if (!e.isClientError()) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
        Errors.setCurrentErrorStatus(e.errorStatus());
        responseObserver.onError(e);
    }
}
