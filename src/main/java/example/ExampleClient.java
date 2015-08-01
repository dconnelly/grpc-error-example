package example;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import io.grpc.*;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import io.grpc.transport.netty.NegotiationType;
import io.grpc.transport.netty.NettyChannelBuilder;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Example client demonstrating various ways to handle application-level error information in error response
 * metadata for different unary call types (blocking, future, async).
 */
public class ExampleClient implements AutoCloseable {
    private final ChannelImpl channel;
    private final ExampleServiceGrpc.ExampleServiceBlockingStub blockingStub;
    private final ExampleServiceGrpc.ExampleServiceFutureStub futureStub;
    private final ExampleServiceGrpc.ExampleServiceStub stub;

    public static ExampleClient forAddress(SocketAddress address) {
        ChannelImpl channel = NettyChannelBuilder.forAddress(address)
            .channelType(NioSocketChannel.class).negotiationType(NegotiationType.PLAINTEXT).build();
        return new ExampleClient(channel);
    }

    private ExampleClient(ChannelImpl channel) {
        this.channel = channel;
        blockingStub = ExampleServiceGrpc.newBlockingStub(channel);
        futureStub = ExampleServiceGrpc.newFutureStub(channel);
        stub = ExampleServiceGrpc.newStub(channel);
    }

    //
    // Demonstrate capturing additional error information from metadata using original stub methods.
    // Definitely not as convenient as using direct unary calls (see below).
    //

    public void generateErrorBlockingStub(ErrorStatus errorStatus) {
        AtomicReference<ErrorStatus> status = new AtomicReference<>();
        try {
            blockingStub.withInterceptors(Errors.clientInterceptor(status::set))
                .generateError(errorRequest(errorStatus));
        } catch (Throwable e) {
            throw ErrorException.fromThrowable(e).withErrorStatus(status.get());
        }
    }

    public ListenableFuture<Empty> generateErrorFutureStub(ErrorStatus errorStatus) {
        AtomicReference<ErrorStatus> status = new AtomicReference<>();
        ListenableFuture<Empty> future = futureStub.withInterceptors(Errors.clientInterceptor(status::set))
            .generateError(errorRequest(errorStatus));
        return Errors.futureWithStatus(status::get, future);
    }

    public void generateErrorAsyncStub(ErrorStatus errorStatus, StreamObserver<Empty> observer) {
        AtomicReference<ErrorStatus> status = new AtomicReference<>();
        stub.withInterceptors(Errors.clientInterceptor(status::set))
            .generateError(errorRequest(errorStatus), Errors.streamObserverWithStatus(status::get, observer));
    }

    //
    // Demonstrate capturing additional error information from metadata using direct unary calls. Clearly
    // much easier than using the stubs directly since a lot of the work can be shared.
    //

    public void generateErrorBlockingUnaryCall(ErrorStatus errorStatus) {
        blockingUnaryCall(ExampleServiceGrpc.METHOD_GENERATE_ERROR, errorRequest(errorStatus));
    }

    public ListenableFuture<Empty> generateErrorFutureUnaryCall(ErrorStatus errorStatus) {
        return futureUnaryCall(ExampleServiceGrpc.METHOD_GENERATE_ERROR, errorRequest(errorStatus));
    }

    public void generateErrorAsyncUnaryCall(ErrorStatus errorStatus, StreamObserver<Empty> observer) {
        asyncUnaryCall(ExampleServiceGrpc.METHOD_GENERATE_ERROR, errorRequest(errorStatus), observer);
    }

    private <ReqT, ResT> ResT blockingUnaryCall(MethodDescriptor<ReqT, ResT> method, ReqT param) {
        AtomicReference<ErrorStatus> status = new AtomicReference<>();
        try {
            return ClientCalls.blockingUnaryCall(capturingCall(status::set, method), param);
        } catch (StatusRuntimeException e) {
            throw ErrorException.forStatus(e.getStatus()).withErrorStatus(status.get());
        }
    }

    public <ReqT, ResT> ListenableFuture<ResT> futureUnaryCall(MethodDescriptor<ReqT, ResT> method, ReqT param) {
        AtomicReference<ErrorStatus> status = new AtomicReference<>();
        ListenableFuture<ResT> future = ClientCalls.futureUnaryCall(capturingCall(status::set, method), param);
        return Errors.futureWithStatus(status::get, future);
    }

    public <ReqT, ResT> void asyncUnaryCall(MethodDescriptor<ReqT, ResT> method,
                                            ReqT param, StreamObserver<ResT> observer) {
        AtomicReference<ErrorStatus> status = new AtomicReference<>();
        ClientCalls.asyncUnaryCall(capturingCall(status::set, method),
            param, Errors.streamObserverWithStatus(status::get, observer));
    }

    public <ReqT, ResT> ClientCall<ReqT, ResT> capturingCall(Consumer<ErrorStatus> statusConsumer,
                                                             MethodDescriptor<ReqT, ResT> method) {
        return Errors.capturingCall(channel.newCall(method, CallOptions.DEFAULT), statusConsumer);
    }

    private static Example.GenerateErrorRequest errorRequest(ErrorStatus errorStatus) {
        return Example.GenerateErrorRequest.newBuilder()
            .setCode(errorStatus.code().toString())
            .setMessage(Strings.nullToEmpty(errorStatus.message()))
            .build();
    }

    @Override
    public void close() {
        channel.shutdownNow();
    }
}
