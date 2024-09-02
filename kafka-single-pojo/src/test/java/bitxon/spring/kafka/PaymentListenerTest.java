package bitxon.spring.kafka;

import bitxon.spring.kafka.config.TestContainersConfig;
import bitxon.spring.kafka.config.TestUtilsConfig;
import bitxon.spring.kafka.listener.PaymentListener;
import bitxon.spring.kafka.model.Payment;
import bitxon.spring.kafka.utils.KafkaWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.time.Duration;

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
class PaymentListenerTest {
    public static final Duration DELAY = Duration.ofMillis(1_000);
    public static final Duration TIMEOUT = Duration.ofMillis(3_000);

    @Qualifier("paymentKafkaWriter")
    @Autowired
    KafkaWriter kafkaWriter;
    @Autowired
    PaymentListener paymentListener;

    @BeforeEach
    void beforeEach() {
        paymentListener.getAttempt().set(0);
        paymentListener.getRepository().clear();
    }

    @Test
    void validRequestWithObject() {
        kafkaWriter.send(new Payment("Msg", 100));

        await().atMost(TIMEOUT).untilAsserted(() ->
            verify(1, 1)
        );
    }

    @Test
    void validRequestWithString() {
        kafkaWriter.send("{\"message\": \"Msg\", \"amount\": 100}");

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
        kafkaWriter.send("{\"message\": \"Msg\", \"amount\": \"not-a-number\"}");

        await().pollDelay(DELAY).untilAsserted(() ->
            verify(0, 0)
        );
    }

    @Test
    void invalidFieldValue() {
        kafkaWriter.send(new Payment("Msg", -1));

        await().pollDelay(DELAY).untilAsserted(() ->
            verify(0, 0)
        );
    }


    @Test
    void failedWithRetry() {
        kafkaWriter.send(new Payment(FAIL_RETRY, 100));

        await().pollDelay(DELAY).untilAsserted(() ->
            verify(6, 0)
        );
    }

    @Test
    void failedButNoRetry() {
        kafkaWriter.send(new Payment(FAIL, 100));

        await().pollDelay(DELAY).untilAsserted(() ->
            verify(1, 0)
        );
    }

    private void verify(int expectedAttempts, int expectedResultRecords) {
        assertThat(paymentListener.getAttempt().get()).as("Number of attempts")
            .isEqualTo(expectedAttempts);
        assertThat(paymentListener.getRepository()).as("Number entities processed")
            .hasSize(expectedResultRecords);
    }

}