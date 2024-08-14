package bitxon.spring.kafka.listener;

import bitxon.spring.kafka.exception.CustomNonRetryableException;
import bitxon.spring.kafka.exception.CustomRetryableException;
import bitxon.spring.kafka.model.Shipment;
import bitxon.spring.kafka.utils.AttemptTracker;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;
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
        concurrency = "1"
    )
    public void handleShipment(@Payload @Valid List<Shipment> shipments) {
        log.info("Shipment messages: {}", shipments);
        attempt.put(shipments);

        for (int index = 0; index < shipments.size(); index++) {
            try {
                handleShipment(shipments.get(index));
            } catch (Exception exception) {
                log.error("Failed on record with index '{}'", index);
                throw new BatchListenerFailedException("Batch Error", exception, index);
            }
        }
    }

    private void handleShipment(Shipment shipment) {
        switch (shipment.address()) {
            case FAIL_RETRY -> throw new CustomRetryableException(FAIL_RETRY);
            case FAIL -> throw new CustomNonRetryableException(FAIL);
        }
        repository.add(shipment);
    }

}
