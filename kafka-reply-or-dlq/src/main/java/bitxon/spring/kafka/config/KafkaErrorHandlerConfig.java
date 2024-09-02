package bitxon.spring.kafka.config;

import bitxon.spring.kafka.exception.CustomNonRetryableException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@Configuration
public class KafkaErrorHandlerConfig {

    /**
     * This will override CommonErrorHandler specified in:
     * {@link org.springframework.boot.autoconfigure.kafka.KafkaAnnotationDrivenConfiguration#kafkaListenerContainerFactoryConfigurer KafkaAnnotationDrivenConfiguration}
     * <p>
     * Default non-retryable exceptions could be found in
     * {@link org.springframework.kafka.listener.ExceptionClassifier#defaultFatalExceptionsList ExceptionClassifier}
     * <p>
     * If you want explicitly specify only retryable exception use
     * {@link DefaultErrorHandler#defaultFalse defaultFalse()} and
     * {@link DefaultErrorHandler#addRetryableExceptions addRetryableExceptions(...)}
     */
    @Bean
    public CommonErrorHandler commonErrorHandler(DeadLetterPublishingRecoverer dlqRecoverer) {
        BackOff backOff = new FixedBackOff(100, 5);

        var errorHandler = new DefaultErrorHandler(dlqRecoverer, backOff);
        errorHandler.addNotRetryableExceptions(CustomNonRetryableException.class);

        return errorHandler;
    }

}
