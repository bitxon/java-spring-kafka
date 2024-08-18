package bitxon.spring.kafka.exception;

public class CustomRetryableException extends RuntimeException {
    public CustomRetryableException() {
    }

    public CustomRetryableException(String message) {
        super(message);
    }
}
