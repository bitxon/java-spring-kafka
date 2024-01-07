package bitxon.spring.kafka.listener;

import bitxon.spring.kafka.exception.CustomNonRetryableException;
import bitxon.spring.kafka.exception.CustomRetryableException;
import bitxon.spring.kafka.model.Payment;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
public class PaymentListener {
    public static final String FAIL_RETRY = "Fail & Retry";
    public static final String FAIL = "Fail";

    public final AtomicInteger attempt = new AtomicInteger();
    public final ConcurrentLinkedQueue<Payment> repository = new ConcurrentLinkedQueue<>();

    @KafkaListener(
        topics = "payment",
        properties = "spring.json.value.default.type=bitxon.spring.kafka.model.Payment",
        concurrency = "3"
    )
    public void handlePayment(@Payload @Valid Payment payment) {
        log.info("Payment message: {}", payment);
        attempt.incrementAndGet();

        switch (payment.message()) {
            case FAIL_RETRY -> throw new CustomRetryableException(FAIL_RETRY);
            case FAIL -> throw new CustomNonRetryableException(FAIL);
        }

        repository.add(payment);
    }

}
