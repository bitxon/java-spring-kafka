package bitxon.spring.kafka;

import bitxon.spring.kafka.config.TestContainersConfig;
import bitxon.spring.kafka.config.TestUtilsConfig;
import bitxon.spring.kafka.listener.InvoiceForwardingListener;
import bitxon.spring.kafka.model.Invoice;
import bitxon.spring.kafka.model.InvoiceProcessed;
import bitxon.spring.kafka.utils.KafkaReader;
import bitxon.spring.kafka.utils.KafkaWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.util.function.Predicate;

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
class InvoiceForwardingListenerTest {
    public static final Duration DELAY = Duration.ofMillis(1_000);
    public static final Duration TIMEOUT = Duration.ofMillis(3_000);

    @Autowired
    KafkaWriter kafkaWriter;
    @Autowired
    KafkaReader<InvoiceProcessed> outputReader;
    @Autowired
    KafkaReader<String> dlqReader;
    @Autowired
    InvoiceForwardingListener invoiceForwardingListener;

    @BeforeEach
    void beforeEach() {
        invoiceForwardingListener.getAttempt().set(0);
        invoiceForwardingListener.getRepository().clear();
        outputReader.clear();
        dlqReader.clear();
    }

    @Test
    void validRequestWithObject() {
        // given
        var input = new Invoice(1, "Msg A");
        var output = new InvoiceProcessed(1, "Msg A");

        // when
        kafkaWriter.send(input);

        // then
        await().atMost(TIMEOUT).untilAsserted(() -> {
            verify(1, 1);
            assertThat(outputReader.find(all())).singleElement().isEqualTo(output);
            assertThat(dlqReader.find(all())).isEmpty();
        });
    }

    @Test
    void validRequestWithString() {
        // given
        var input = "{\"id\": 1, \"message\": \"Msg B\"}";
        var output = new InvoiceProcessed(1, "Msg B");

        // when
        kafkaWriter.send(input);

        // then
        await().atMost(TIMEOUT).untilAsserted(() -> {
            verify(1, 1);
            assertThat(outputReader.find(all())).singleElement().isEqualTo(output);
            assertThat(dlqReader.find(all())).isEmpty();
        });
    }

    @Test
    void invalidJsonFormat() {
        // given
        var input = "{\"invalid-json {";

        // when
        kafkaWriter.send(input);

        // then
        await().pollDelay(DELAY).untilAsserted(() -> {
            verify(0, 0);
            assertThat(outputReader.find(all())).isEmpty();
            assertThat(dlqReader.find(all())).singleElement().isEqualTo(input);
        });
    }

    @Test
    void invalidFieldType() {
        // given
        var input = "{\"id\": \"not-a-number\", \"message\": \"Msg C\"}";

        // when
        kafkaWriter.send(input);

        // then
        await().pollDelay(DELAY).untilAsserted(() -> {
            verify(0, 0);
            assertThat(outputReader.find(all())).isEmpty();
            assertThat(dlqReader.find(all())).singleElement().isEqualTo(input);
        });
    }

    @Test
    void invalidFieldValue() {
        // given
        var input = new Invoice(-1, "Msg D");
        var dlqOutput = "{\"id\":-1,\"message\":\"Msg D\"}";

        // when
        kafkaWriter.send(input);

        // then
        await().pollDelay(DELAY).untilAsserted(() -> {
            verify(0, 0);
            assertThat(outputReader.find(all())).isEmpty();
            assertThat(dlqReader.find(all())).singleElement().isEqualTo(dlqOutput);
        });
    }


    @Test
    void failedWithRetry() {
        // given
        var input = new Invoice(1, FAIL_RETRY);
        var dlqOutput = "{\"id\":1,\"message\":\"Fail & Retry\"}";

        // when
        kafkaWriter.send(input);

        // then
        await().pollDelay(DELAY).untilAsserted(() -> {
            verify(6, 0);
            assertThat(outputReader.find(all())).isEmpty();
            assertThat(dlqReader.find(all())).singleElement().isEqualTo(dlqOutput);
        });
    }

    @Test
    void failedButNoRetry() {
        // given
        var input = new Invoice(1, FAIL);
        var dlqOutput = "{\"id\":1,\"message\":\"Fail\"}";

        // when
        kafkaWriter.send(input);

        // then
        await().pollDelay(DELAY).untilAsserted(() -> {
            verify(1, 0);
            assertThat(outputReader.find(all())).isEmpty();
            assertThat(dlqReader.find(all())).singleElement().isEqualTo(dlqOutput);
        });
    }


    private void verify(int expectedAttempts, int expectedResultRecords) {
        assertThat(invoiceForwardingListener.getAttempt().get()).as("Number of attempts")
            .isEqualTo(expectedAttempts);
        assertThat(invoiceForwardingListener.getRepository()).as("Number entities processed")
            .hasSize(expectedResultRecords);
    }


    private static Predicate<InvoiceProcessed> byId(int id) {
        return it -> it.id() == id;
    }

    private static <T> Predicate<T> all() {
        return it -> true;
    }

}