package bitxon.spring.kafka.listener;

import bitxon.spring.kafka.exception.CustomNonRetryableException;
import bitxon.spring.kafka.exception.CustomRetryableException;
import bitxon.spring.kafka.model.Shipment;
import bitxon.spring.kafka.utils.AttemptTracker;
import jakarta.validation.Validator;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static bitxon.spring.kafka.exception.ErrorType.FAIL;
import static bitxon.spring.kafka.exception.ErrorType.FAIL_RETRY;


@Slf4j
@Service
@RequiredArgsConstructor
public class ShipmentBatchListener {
    @Getter private final AttemptTracker<List<Shipment>> attempt = new AttemptTracker<>();
    @Getter private final ConcurrentLinkedQueue<Shipment> repository = new ConcurrentLinkedQueue<>();

    private final Validator validator;

    @KafkaListener(
        batch = "true",
        topics = "shipment"
    )
    // https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html#batch-listener-conv-errors
    public void handleShipment(@Payload List<Shipment> shipments,
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
                validate(shipment);
            } catch (Exception ex) {
                throw new BatchListenerFailedException("Validation Error", ex, index);
            }

            try {
                handleShipment(shipment);
            } catch (Exception ex) {
                throw new BatchListenerFailedException("Processing Error", ex, index);
            }
        }
    }

    private void validate(Shipment shipment) {
        if (shipment == null) {
            throw new CustomNonRetryableException("Entity must not be null");
        }
        var errors = validator.validate(shipment);
        if (!CollectionUtils.isEmpty(errors)) {
            var message = errors.stream()
                .map(it -> "%s %s".formatted(it.getPropertyPath().toString(), it.getMessage()))
                .collect(Collectors.joining(", "));
            throw new CustomNonRetryableException(message);
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
