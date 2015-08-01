package example;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;

/**
 * Extends StatusRuntimeException with support for additional application error status information. Additional
 * error information will be returned in error response metadata.
 */
public class ErrorException extends StatusRuntimeException {
    private final ErrorStatus errorStatus;

    public static ErrorException fromThrowable(Throwable e) {
        if (e instanceof ErrorException) {
            return (ErrorException) e;
        } else if (e instanceof StatusRuntimeException) {
            return forStatus(((StatusRuntimeException) e).getStatus());
        } else if (e instanceof StatusException) {
            return forStatus(((StatusException) e).getStatus());
        } else {
            return forStatus(Status.INTERNAL.withCause(e));
        }
    }

    public static ErrorException forStatus(Status status) {
        return new ErrorException(status, null);
    }

    public static ErrorException forErrorStatus(ErrorStatus errorStatus) {
        Status status = toStatus(errorStatus.code())
            .withDescription(errorStatus.message());
        return forStatus(status).withErrorStatus(errorStatus);
    }

    private static Status toStatus(ErrorStatus.Code code) {
        switch (code) {
        case accountNotFound:
        case deviceNotFound:
            return Status.NOT_FOUND;
        case unknown:
            return Status.UNKNOWN;
        default:
            return Status.INTERNAL;
        }
    }

    private ErrorException(Status status, ErrorStatus errorStatus) {
        super(status);
        this.errorStatus = errorStatus;
    }

    public ErrorStatus errorStatus() {
        return errorStatus;
    }

    public ErrorException withErrorStatus(ErrorStatus errorStatus) {
        return new ErrorException(getStatus(), errorStatus);
    }

    public boolean isClientError() {
        switch (getStatus().getCode()) {
            case CANCELLED:
            case INVALID_ARGUMENT:
            case NOT_FOUND:
            case ALREADY_EXISTS:
            case PERMISSION_DENIED:
            case FAILED_PRECONDITION:
            case UNAUTHENTICATED:
                return true;
            default:
                return false;
            }
    }
}
