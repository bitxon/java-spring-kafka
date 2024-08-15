package bitxon.spring.kafka.listener;

import bitxon.spring.kafka.exception.CustomNonRetryableException;
import bitxon.spring.kafka.exception.CustomRetryableException;
import bitxon.spring.kafka.model.Shipment;
import bitxon.spring.kafka.utils.AttemptTracker;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;


@Slf4j
@Service
public class ShipmentBatchListener {
    public static final String FAIL_RETRY = "Fail & Retry";
    public static final String FAIL = "Fail";

    public final AttemptTracker<List<Shipment>> attempt = new AttemptTracker<>();
    public final ConcurrentLinkedQueue<Shipment> repository = new ConcurrentLinkedQueue<>();

    @KafkaListener(
        batch = "true",
        topics = "shipment",
        properties = "spring.json.value.default.type=bitxon.spring.kafka.model.Shipment",
        concurrency = "1",
        containerFactory = "shipmentKafkaListenerContainerFactory"
    )
    // https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html#batch-listener-conv-errors
    public void handleShipment(@Payload @Valid List<Shipment> shipments,
                               @Header(KafkaHeaders.CONVERSION_FAILURES) List<ConversionException> exceptions) {
        log.info("Shipment messages: {}, exceptions: {}", shipments, exceptions);
        attempt.put(shipments);

        for (int index = 0; index < shipments.size(); index++) {
            var shipment = shipments.get(index);
            var exception = exceptions.get(index);

            if (shipment == null && exception != null) {
                throw new BatchListenerFailedException("Conversion Error", exception, index);
            }

            try {
                handleShipment(shipment);
            } catch (Exception ex) {
                throw new BatchListenerFailedException("Processing Error", ex, index);
            }
        }
    }

    private void handleShipment(Shipment shipment) {
        switch (Optional.ofNullable(shipment.address()).orElse("N/A")) {
            case FAIL_RETRY -> throw new CustomRetryableException(FAIL_RETRY);
            case FAIL -> throw new CustomNonRetryableException(FAIL);
        }
        repository.add(shipment);
    }

}
