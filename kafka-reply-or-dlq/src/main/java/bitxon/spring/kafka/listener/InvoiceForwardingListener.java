package bitxon.spring.kafka.listener;

import bitxon.spring.kafka.exception.CustomNonRetryableException;
import bitxon.spring.kafka.exception.CustomRetryableException;
import bitxon.spring.kafka.model.Invoice;
import bitxon.spring.kafka.model.InvoiceProcessed;
import jakarta.validation.Valid;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static bitxon.spring.kafka.exception.ErrorType.FAIL;
import static bitxon.spring.kafka.exception.ErrorType.FAIL_RETRY;

@Slf4j
@Service
public class InvoiceForwardingListener {
    @Getter private final AtomicInteger attempt = new AtomicInteger();
    @Getter private final ConcurrentLinkedQueue<Invoice> repository = new ConcurrentLinkedQueue<>();

    @KafkaListener(
        topics = "invoice-request",
        properties = "spring.json.value.default.type=bitxon.spring.kafka.model.Invoice"
    )
    @SendTo("invoice-response")
    // https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/annotation-send-to.html
    public InvoiceProcessed handleInvoice(@Payload @Valid Invoice invoice) {
        log.info("Invoice message: {}", invoice);
        attempt.incrementAndGet();

        switch (invoice.message()) {
            case FAIL_RETRY -> throw new CustomRetryableException(FAIL_RETRY);
            case FAIL -> throw new CustomNonRetryableException(FAIL);
        }

        repository.add(invoice);
        return new InvoiceProcessed(invoice.id(), invoice.message());
    }

}
