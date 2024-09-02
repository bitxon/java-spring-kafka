package bitxon.spring.kafka;

import bitxon.spring.kafka.utils.KafkaWriter;
import bitxon.spring.kafka.config.TestContainersConfig;
import bitxon.spring.kafka.config.TestUtilsConfig;
import bitxon.spring.kafka.listener.OrderListener;
import bitxon.spring.kafka.model.Order;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@Import({
    TestContainersConfig.class,
    TestUtilsConfig.class
})
@SpringBootTest(webEnvironment = RANDOM_PORT)
class OrderListenerTest {
    public static final Duration DELAY = Duration.ofMillis(2_100);
    public static final Duration TIMEOUT = Duration.ofMillis(3_000);

    @Qualifier("orderKafkaWriter")
    @Autowired
    KafkaWriter kafkaWriter;
    @Autowired
    OrderListener orderListener;

    @BeforeEach
    void beforeEach() {
        orderListener.getAttempt().set(0);
        orderListener.getRepository().clear();
    }

    @Test
    void validRequestWithObject() {
        kafkaWriter.send(new Order("Product1", 1));

        await().atMost(TIMEOUT).untilAsserted(() ->
            verify(1, 1)
        );
    }

    @Test
    void validRequestWithString() {
        kafkaWriter.send("{\"product\": \"Order1\", \"quantity\": 1}");

        await().atMost(TIMEOUT).untilAsserted(() ->
            verify(1, 1)
        );
    }

    @Test
    void invalidJsonFormat() {
        kafkaWriter.send("{\"invalid-json {");

        await().pollDelay(DELAY).untilAsserted(() ->
            verify(0, 0)
        );
    }

    @Test
    void invalidFieldType() {
        kafkaWriter.send("{\"product\": \"Order1\", \"quantity\": \"not-a-number\"}");

        await().pollDelay(DELAY).untilAsserted(() ->
            verify(0, 0)
        );
    }

    @Test
    void invalidFieldValue() {
        kafkaWriter.send(new Order(null, 1));

        await().pollDelay(DELAY).untilAsserted(() ->
            verify(0, 0)
        );
    }

    private void verify(int expectedAttempts, int expectedResultRecords) {
        assertThat(orderListener.getAttempt().get()).as("Number of attempts")
            .isEqualTo(expectedAttempts);
        assertThat(orderListener.getRepository()).as("Number entities processed")
            .hasSize(expectedResultRecords);
    }

}