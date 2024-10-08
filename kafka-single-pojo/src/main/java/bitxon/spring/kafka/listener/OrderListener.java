package bitxon.spring.kafka.listener;

import bitxon.spring.kafka.model.Order;
import jakarta.validation.Valid;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
public class OrderListener {
    @Getter private final AtomicInteger attempt = new AtomicInteger();
    @Getter private final ConcurrentLinkedQueue<Order> repository = new ConcurrentLinkedQueue<>();

    @KafkaListener(
        topics = "order",
        properties = "spring.json.value.default.type=bitxon.spring.kafka.model.Order",
        concurrency = "3"
    )
    public void handleOrder(@Payload @Valid Order order) {
        log.info("Order message: {}", order);
        attempt.incrementAndGet();

        try {
            Thread.sleep(2000); // pretend that this is long-running operation
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        repository.add(order);
    }

}
