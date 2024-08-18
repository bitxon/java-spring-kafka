package bitxon.spring.kafka;

import bitxon.spring.kafka.config.TestContainersConfig;
import bitxon.spring.kafka.config.TestProducerConfig;
import bitxon.spring.kafka.listener.ShipmentBatchListener;
import bitxon.spring.kafka.model.Shipment;
import bitxon.spring.kafka.utils.KafkaWriter;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static bitxon.spring.kafka.exception.ErrorType.FAIL;
import static bitxon.spring.kafka.exception.ErrorType.FAIL_RETRY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@Import({
    TestContainersConfig.class,
    TestProducerConfig.class
})
@SpringBootTest(webEnvironment = RANDOM_PORT)
class ShipmentBatchListenerTest {
    private static final Duration DELAY = Duration.ofMillis(1_500);
    private static final Duration TIMEOUT = Duration.ofMillis(3_000);
    private static final List<Shipment> NONE = List.of();

    @Autowired
    KafkaWriter kafkaWriter;
    @Autowired
    ShipmentBatchListener shipmentBatchListener;

    @BeforeEach
    void beforeEach() {
        shipmentBatchListener.getAttempt().clear();
        shipmentBatchListener.getRepository().clear();
    }

    @Test
    void validRequestWithObject() throws JsonProcessingException {
        kafkaWriter.send("shipment", new Shipment("1", 1));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertAttemptsNumber(1);
            assertProcessedNumber(1);
        });
    }

    @Test
    void validRequestWithObjectBatch() throws JsonProcessingException {
        // given
        var entity1 = new Shipment("1", 1);
        var entity2 = new Shipment("2", 2);

        // when
        kafkaWriter.send("shipment", List.of(entity1, entity2));

        // then
        await().pollDelay(DELAY).untilAsserted(() -> {
            assertAttemptsNumber(1);
            assertProcessedNumber(2);
        });
    }

    @Test
    void validRequestWithString() throws JsonProcessingException {
        kafkaWriter.send("shipment", "{\"address\": \"Msg\", \"trackingNumber\": 100}");

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertAttemptsNumber(1);
            assertProcessedNumber(1);
        });
    }

    @Test
    void invalidJsonFormat() throws JsonProcessingException {
        kafkaWriter.send("shipment", "{\"invalid-json {");

        await().pollDelay(DELAY).untilAsserted(() -> {
            assertAttemptsNumber(1);
            assertProcessedNumber(0);
        });
    }

    @Test
    void invalidFieldType() throws JsonProcessingException {
        kafkaWriter.send("shipment", "{\"address\": \"Msg\", \"trackingNumber\": \"not-a-number\"}");

        await().pollDelay(DELAY).untilAsserted(() -> {
            assertAttemptsNumber(1);
            assertProcessedNumber(0);
        });
    }

//    @Disabled // TODO research why @Valid is not Working
    @Test
    void invalidFieldValue() throws JsonProcessingException {
        kafkaWriter.send("shipment", new Shipment("Msg", -1));

        await().pollDelay(DELAY).untilAsserted(() -> {
            assertAttemptsNumber(0);
            assertProcessedNumber(0);
        });
    }

    @Test
    void failedWithRetry() throws JsonProcessingException {
        var entity1 = new Shipment(FAIL_RETRY, 100);

        kafkaWriter.send("shipment", entity1);

        await().pollDelay(DELAY).untilAsserted(() -> {
            assertAttempts(Map.of(
                1, List.of(entity1),
                2, List.of(entity1),
                3, List.of(entity1),
                4, List.of(entity1),
                5, List.of(entity1),
                6, List.of(entity1)
            ));
            assertProcessed(NONE);
        });
    }

    @Test
    void failedWithRetryBatch() throws JsonProcessingException {
        var entity1 = new Shipment("1", 1);
        var entity2 = new Shipment(FAIL_RETRY, 2);
        var entity3 = new Shipment("3", 3);
        var entity4 = new Shipment("4", 4);

        kafkaWriter.send("shipment", List.of(entity1, entity2, entity3, entity4));

        await().pollDelay(DELAY).untilAsserted(() -> {
            assertAttempts(Map.of(
                1, List.of(entity1, entity2, entity3, entity4), // Process #1, Failed on #2 -> Retry
                2, List.of(entity2, entity3, entity4),          // Retry-1: Failed on #2
                3, List.of(entity2, entity3, entity4),          // Retry-2: Failed on #2
                4, List.of(entity2, entity3, entity4),          // Retry-3: Failed on #2
                5, List.of(entity2, entity3, entity4),          // Retry-4: Failed on #2
                6, List.of(entity2, entity3, entity4),          // Retry-5: Failed on #2 -> No more retry
                7, List.of(entity3, entity4)                    // Process #3, #4
            ));
            assertProcessed(List.of(entity1, entity3, entity4));
        });
    }

    @Test
    void failedButNoRetry() throws JsonProcessingException {
        var entity1 = new Shipment(FAIL, 100);
        kafkaWriter.send("shipment", entity1);

        await().pollDelay(DELAY).untilAsserted(() -> {
            assertAttempts(Map.of(1, List.of(entity1)));
            assertProcessed(NONE);
        });
    }

    @Test
    void failedButNoRetryBatch() throws JsonProcessingException {
        // given
        var entity1 = new Shipment("1", 1);
        var entity2 = new Shipment(FAIL, 2);
        var entity3 = new Shipment("3", 3);
        var entity4 = new Shipment("4", 4);

        // when
        kafkaWriter.send("shipment", List.of(entity1, entity2, entity3, entity4));

        // then
        await().pollDelay(DELAY).untilAsserted(() -> {
            assertAttempts(Map.of(
                1, List.of(entity1, entity2, entity3, entity4),
                2, List.of(entity3, entity4)
            ));
            assertProcessed(List.of(entity1, entity3, entity4));
        });
    }


    private void assertAttempts(Map<Integer, List<Shipment>> expectedAttempts) {
        assertThat(shipmentBatchListener.getAttempt().getAll()).as("Check attempts")
            .containsExactlyInAnyOrderEntriesOf(expectedAttempts);
    }

    private void assertAttemptsNumber(int expectedNumberOfAttempts) {
        assertThat(shipmentBatchListener.getAttempt().getAll()).as("Check attempts")
            .hasSize(expectedNumberOfAttempts);
    }

    private void assertProcessed(List<Shipment> expectedRecords) {
        if (expectedRecords == null || expectedRecords.isEmpty()) {
            assertThat(shipmentBatchListener.getRepository()).as("Check entities processed")
                .isEmpty();
        } else {
            assertThat(shipmentBatchListener.getRepository()).as("Check entities processed")
                .containsExactlyInAnyOrderElementsOf(expectedRecords);
        }
    }

    private void assertProcessedNumber(int expectedNumberOfProcessedRecords) {
        assertThat(shipmentBatchListener.getRepository()).as("Check entities processed")
            .hasSize(expectedNumberOfProcessedRecords);
    }

}