package bitxon.spring.kafka;

import bitxon.spring.kafka.config.TestContainersConfig;
import bitxon.spring.kafka.config.TestProducerConfig;
import bitxon.spring.kafka.listener.PaymentListener;
import bitxon.spring.kafka.model.Payment;
import bitxon.spring.kafka.utils.KafkaWriter;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.time.Duration;

import static bitxon.spring.kafka.listener.PaymentListener.FAIL;
import static bitxon.spring.kafka.listener.PaymentListener.FAIL_RETRY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@Import({
    TestContainersConfig.class,
    TestProducerConfig.class
})
@SpringBootTest(webEnvironment = RANDOM_PORT)
class PaymentKafkaTest {

    public static final Duration DELAY = Duration.ofMillis(1_000);
    public static final Duration TIMEOUT = Duration.ofMillis(3_000);

    @Autowired
    KafkaWriter kafkaWriter;

    @Autowired
    PaymentListener paymentListener;

    @BeforeEach
    void beforeEach() {
        paymentListener.attempt.set(0);
        paymentListener.repository.clear();
    }

    @AfterEach
    void afterEach() {
        paymentListener.attempt.set(0);
        paymentListener.repository.clear();
    }

    @Test
    void validRequestWithObject() throws JsonProcessingException {
        kafkaWriter.send("payment", new Payment("Msg", 100));

        await().atMost(TIMEOUT).untilAsserted(() ->
            verify(1, 1)
        );
    }

    @Test
    void validRequestWithString() throws JsonProcessingException {
        kafkaWriter.send("payment", "{\"message\": \"Msg\", \"amount\": 100}");

        await().atMost(TIMEOUT).untilAsserted(() ->
            verify(1, 1)
        );
    }

    @Test
    void invalidJsonFormat() throws JsonProcessingException {
        kafkaWriter.send("payment", "{\"invalid-json {");

        await().pollDelay(DELAY).untilAsserted(() ->
            verify(0, 0)
        );
    }

    @Test
    void invalidFieldType() throws JsonProcessingException {
        kafkaWriter.send("payment", "{\"message\": \"Msg\", \"amount\": \"not-a-number\"}");

        await().pollDelay(DELAY).untilAsserted(() ->
            verify(0, 0)
        );
    }

    @Test
    void invalidFieldValue() throws JsonProcessingException {
        kafkaWriter.send("payment", new Payment("Msg", -1));

        await().pollDelay(DELAY).untilAsserted(() ->
            verify(0, 0)
        );
    }


    @Test
    void failedWithRetry() throws JsonProcessingException {
        kafkaWriter.send("payment", new Payment(FAIL_RETRY, 100));

        await().pollDelay(DELAY).untilAsserted(() ->
            verify(6, 0)
        );
    }

    @Test
    void failedButNoRetry() throws JsonProcessingException {
        kafkaWriter.send("payment", new Payment(FAIL, 100));

        await().pollDelay(DELAY).untilAsserted(() ->
            verify(1, 0)
        );
    }

    private void verify(int expectedAttempts, int expectedResultRecords) {
        assertThat(paymentListener.attempt.get()).as("Number of attempts")
            .isEqualTo(expectedAttempts);
        assertThat(paymentListener.repository).as("Number entities processed")
            .hasSize(expectedResultRecords);
    }

}