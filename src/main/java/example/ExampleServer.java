package example;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.protobuf.Empty;
import io.grpc.ServerImpl;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.grpc.transport.netty.NettyServerBuilder;

/**
 * Example server and service demonstrating various ways to included application-level error information in error
 * response metadata.
 */
public class ExampleServer extends AbstractIdleService {
    private final int port;
    private ServerImpl server;

    public static ExampleServer forPort(int port) {
        return new ExampleServer(port);
    }

    private ExampleServer(int port) {
        this.port = port;
    }

    @Override
    protected void startUp() throws Exception {
        ServerServiceDefinition service = ExampleServiceGrpc.bindService(new ExampleService());
        server = NettyServerBuilder.forPort(8080)
            .addService(ServerInterceptors.intercept(service, Errors.serverInterceptor())).build().start();
    }

    @Override
    protected void shutDown() throws Exception {
        server.shutdownNow();
        server.awaitTerminated();
    }

    private static class ExampleService implements ExampleServiceGrpc.ExampleService {
        @Override
        public void generateError(Example.GenerateErrorRequest request, StreamObserver<Empty> responseObserver) {
            try {
                ErrorStatus.Code code = ErrorStatus.Code.fromString(request.getCode())
                    .orElseThrow(Status.INVALID_ARGUMENT::asRuntimeException);
                ErrorStatus status = ErrorStatus.forCode(code).withMessage(Strings.emptyToNull(request.getMessage()));
                throw GrpcException.forErrorStatus(status);
            } catch (Throwable e) {
                Errors.handleError(e, responseObserver);
            }
        }
    }
}
