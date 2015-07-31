package example;

import io.grpc.Metadata;

import java.util.Objects;
import java.util.Optional;

/**
 * Additional application-level error information to be included in error responses. Encoded as metadata header fields.
 */
public class ErrorStatus {
    public enum Code {
        unknown, accountNotFound, deviceNotFound, loginRequired;

        public static Optional<Code> fromString(String s) {
            try {
                return Optional.of(Code.valueOf(s));
            } catch (IllegalArgumentException e) {
                return Optional.empty();
            }
        }
    }

    private final Code code;
    private final String message;

    private static final Metadata.Key<Code> codeKey =
        Metadata.Key.of("error-code-bin", Marshallers.enumMarshaller(Code.class, Code.unknown));
    private static final Metadata.Key<String> messageKey =
        Metadata.Key.of("error-message-bin", Marshallers.utf8Marshaller());

    public static ErrorStatus forCode(Code code) {
        Objects.requireNonNull(code, "code");
        return new ErrorStatus(code, null);
    }

    public static Optional<ErrorStatus> fromMetadata(Metadata md) {
        Code code = md.get(codeKey);
        return code != null ? Optional.of(new ErrorStatus(code, md.get(messageKey))) : Optional.empty();
    }

    private ErrorStatus(Code code, String message) {
        this.code = code;
        this.message = message;
    }

    public ErrorStatus withMessage(String message) {
        return new ErrorStatus(code, message);
    }

    public Code code() {
        return code;
    }

    public String message() {
        return message;
    }

    public void addHeaders(Metadata md) {
        md.put(codeKey, code);
        if (message != null) {
            md.put(messageKey, message);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ErrorStatus that = (ErrorStatus) o;
        return Objects.equals(code, that.code) && Objects.equals(message, that.message);
    }
}
