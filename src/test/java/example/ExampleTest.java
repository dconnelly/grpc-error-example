package example;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamRecorder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Tests various ways to handle application-level error information in error response metadata for both stub-based
 * and direct unary call types.
 */
public class ExampleTest {
    private static ExampleServer server;
    private static ExampleClient client;

    private static final ErrorStatus accountNotFound =
        ErrorStatus.forCode(ErrorStatus.Code.accountNotFound).withMessage("Account not found");

    @BeforeClass
    public static void init() {
        server = ExampleServer.forPort(8080);
        server.startAsync().awaitRunning();
        client = ExampleClient.forAddress(new InetSocketAddress(8080));
    }

    @AfterClass
    public static void destroy() {
        Optional.ofNullable(client).ifPresent(ExampleClient::close);
        Optional.ofNullable(server).ifPresent(s -> s.stopAsync().awaitTerminated());
    }

    @Test
    public void blockingStub() {
        try {
            client.generateErrorBlockingStub(accountNotFound);
            fail();
        } catch (ErrorException e) {
            assertEquals(e.getStatus().getCode(), Status.Code.NOT_FOUND);
            assertEquals(accountNotFound, e.errorStatus());
        }
    }

    @Test
    public void futureStub() {
        ListenableFuture<Empty> future = client.generateErrorFutureStub(accountNotFound);
        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof ErrorException);
            assertEquals(accountNotFound, ((ErrorException) e.getCause()).errorStatus());
        } catch (InterruptedException e) {
            fail();
        }
    }

    @Test
    public void asyncStub() throws Exception {
        StreamRecorder<Empty> recorder = StreamRecorder.create();
        client.generateErrorAsyncStub(accountNotFound, recorder);
        recorder.awaitCompletion(5, TimeUnit.SECONDS);
        assertTrue(recorder.getError() instanceof ErrorException);
        assertEquals(accountNotFound, ((ErrorException) recorder.getError()).errorStatus());
    }

    @Test
    public void blockingUnaryCall() {
        try {
            client.generateErrorBlockingUnaryCall(accountNotFound);
            fail();
        } catch (ErrorException e) {
            assertEquals(e.getStatus().getCode(), Status.Code.NOT_FOUND);
            assertEquals(accountNotFound, e.errorStatus());
        }
    }

    @Test
    public void futureUnaryCall() {
        ListenableFuture<Empty> future = client.generateErrorFutureUnaryCall(accountNotFound);
        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof ErrorException);
            assertEquals(accountNotFound, ((ErrorException) e.getCause()).errorStatus());
        } catch (InterruptedException e) {
            fail();
        }
    }

    @Test
    public void asyncUnaryCall() throws Exception {
        StreamRecorder<Empty> recorder = StreamRecorder.create();
        client.generateErrorAsyncUnaryCall(accountNotFound, recorder);
        recorder.awaitCompletion(5, TimeUnit.SECONDS);
        assertTrue(recorder.getError() instanceof ErrorException);
        assertEquals(accountNotFound, ((ErrorException) recorder.getError()).errorStatus());
    }
}
