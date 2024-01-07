package bitxon.spring.kafka.exception;

public class CustomNonRetryableException extends RuntimeException {
    public CustomNonRetryableException() {
    }

    public CustomNonRetryableException(String message) {
        super(message);
    }
}
