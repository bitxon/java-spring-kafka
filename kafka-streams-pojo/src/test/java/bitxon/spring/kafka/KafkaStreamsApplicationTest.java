package bitxon.spring.kafka;

import bitxon.spring.kafka.config.TestContainersConfig;
import bitxon.spring.kafka.config.TestUtilsConfig;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@Import({
    TestContainersConfig.class,
    TestUtilsConfig.class
})
@SpringBootTest(webEnvironment = RANDOM_PORT)
class KafkaStreamsApplicationTest {
    public static final Duration DELAY = Duration.ofMillis(1_000);
    public static final Duration TIMEOUT = Duration.ofMillis(3_000);

    @Autowired
    KafkaWriter kafkaWriter;
    @Autowired
    KafkaReader<InvoiceProcessed> kafkaReader;

    @BeforeEach
    void beforeEach() {
        kafkaReader.clear();
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
            assertThat(kafkaReader.find(all())).singleElement().isEqualTo(output);
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
            assertThat(kafkaReader.find(all())).singleElement().isEqualTo(output);
        });
    }


    private static <T> Predicate<T> all() {
        return it -> true;
    }
}
