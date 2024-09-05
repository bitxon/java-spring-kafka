package bitxon.spring.kafka;

import bitxon.spring.kafka.config.TestContainersConfig;
import bitxon.spring.kafka.config.TestUtilsConfig;
import bitxon.spring.kafka.listener.ShipmentBatchV2Listener;
import bitxon.spring.kafka.model.Shipment;
import bitxon.spring.kafka.utils.KafkaWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static bitxon.spring.kafka.exception.ErrorType.FAIL;
import static bitxon.spring.kafka.exception.ErrorType.FAIL_RETRY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@Import({
    TestContainersConfig.class,
    TestUtilsConfig.class
})
@SpringBootTest(webEnvironment = RANDOM_PORT)
class ShipmentBatchV2ListenerTest {
    private static final Duration DELAY = Duration.ofMillis(1_500);
    private static final Duration TIMEOUT = Duration.ofMillis(3_000);
    private static final List<Shipment> NONE = List.of();

    @Autowired
    KafkaWriter kafkaWriter;
    @Autowired
    ShipmentBatchV2Listener shipmentBatchListener;

    @BeforeEach
    void beforeEach() {
        shipmentBatchListener.getAttempt().clear();
        shipmentBatchListener.getRepository().clear();
    }

    @Test
    void validRequestWithObject() {
        // when
        kafkaWriter.send(new Shipment("1", 1));

        // then
        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertAttemptsNumber(1);
            assertProcessedNumber(1);
        });
    }

    @Test
    void validRequestWithObjectBatch() {
        // given
        var entity1 = new Shipment("1", 1);
        var entity2 = new Shipment("2", 2);

        // when
        kafkaWriter.send(List.of(entity1, entity2));

        // then
        await().pollDelay(DELAY).untilAsserted(() -> {
            assertAttemptsNumber(1);
            assertProcessedNumber(2);
        });
    }

    @Test
    void validRequestWithString() {
        // when
        kafkaWriter.send("{\"address\": \"Msg\", \"trackingNumber\": 100}");

        // then
        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertAttemptsNumber(1);
            assertProcessedNumber(1);
        });
    }

    @Test
    void invalidJsonFormat() {
        // when
        kafkaWriter.send("{\"invalid-json {");

        // then
        await().pollDelay(DELAY).untilAsserted(() -> {
            assertAttemptsNumber(1);
            assertProcessedNumber(0);
        });
    }

    @Test
    void invalidJsonFormatBatch() {
        // when
        var entity1 = new Shipment("1", 1);
        var entity2 = "{\"invalid-json {";
        var entity3 = new Shipment("3", 3);
        var entity4 = new Shipment("4", 4);
        kafkaWriter.send(List.of(entity1, entity2, entity3, entity4));

        // then
        await().pollDelay(DELAY).untilAsserted(() -> {
            assertAttempts(Map.of(
                1, Arrays.asList(entity1, null, entity3, entity4), // Process #1, Failed on #2 -> Retry
                2, List.of(entity3, entity4)                    // Process #3, #4
            ));
            assertProcessed(List.of(entity1, entity3, entity4));
        });
    }

    @Test
    void invalidFieldType() {
        // when
        kafkaWriter.send("{\"address\": \"Msg\", \"trackingNumber\": \"not-a-number\"}");

        // then
        await().pollDelay(DELAY).untilAsserted(() -> {
            assertAttemptsNumber(1);
            assertProcessedNumber(0);
        });
    }

    @Test
    void invalidObjectNull() {
        kafkaWriter.send((Shipment) null);

        await().pollDelay(DELAY).untilAsserted(() -> {
            assertAttemptsNumber(1);
            assertProcessedNumber(0);
        });
    }

    @Test
    void invalidFieldValue() {
        kafkaWriter.send(new Shipment("Msg", -1));

        await().pollDelay(DELAY).untilAsserted(() -> {
            assertAttemptsNumber(1);
            assertProcessedNumber(0);
        });
    }

    @Test
    void invalidFieldValueBatch() {
        var entity1 = new Shipment("1", 1);
        var entity2 = new Shipment("2", -1); // invalid
        var entity3 = new Shipment("3", 3);
        var entity4 = new Shipment("4", 4);
        kafkaWriter.send(List.of(entity1, entity2, entity3, entity4));

        await().pollDelay(DELAY).untilAsserted(() -> {
            assertAttempts(Map.of(
                1, Arrays.asList(entity1, entity2, entity3, entity4), // Process #1, Failed on #2 -> Retry
                2, List.of(entity3, entity4)                    // Process #3, #4
            ));
            assertProcessed(List.of(entity1, entity3, entity4));
        });
    }

    @Test
    void failedWithRetry() {
        // given
        var entity1 = new Shipment(FAIL_RETRY, 100);

        // when
        kafkaWriter.send(entity1);

        // then
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
    void failedWithRetryBatch() {
        // given
        var entity1 = new Shipment("1", 1);
        var entity2 = new Shipment(FAIL_RETRY, 2);
        var entity3 = new Shipment("3", 3);
        var entity4 = new Shipment("4", 4);

        // when
        kafkaWriter.send(List.of(entity1, entity2, entity3, entity4));

        // then
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
    void failedButNoRetry() {
        // given
        var entity1 = new Shipment(FAIL, 100);

        // when
        kafkaWriter.send(entity1);

        // then
        await().pollDelay(DELAY).untilAsserted(() -> {
            assertAttempts(Map.of(1, List.of(entity1)));
            assertProcessed(NONE);
        });
    }

    @Test
    void failedButNoRetryBatch() {
        // given
        var entity1 = new Shipment("1", 1);
        var entity2 = new Shipment(FAIL, 2);
        var entity3 = new Shipment("3", 3);
        var entity4 = new Shipment("4", 4);

        // when
        kafkaWriter.send(List.of(entity1, entity2, entity3, entity4));

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