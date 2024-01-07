package bitxon.spring.kafka;

import bitxon.spring.kafka.utils.KafkaWriter;
import bitxon.spring.kafka.config.TestContainersConfig;
import bitxon.spring.kafka.config.TestProducerConfig;
import bitxon.spring.kafka.listener.OrderListener;
import bitxon.spring.kafka.model.Order;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@Import({
    TestContainersConfig.class,
    TestProducerConfig.class
})
@SpringBootTest(webEnvironment = RANDOM_PORT)
class OrderKafkaTest {

    @Autowired
    KafkaWriter kafkaWriter;

    @Autowired
    OrderListener orderListener;

    @AfterEach
    void afterEach() {
        orderListener.repository.clear();
    }

    @Test
    void validRequestWithObject() throws JsonProcessingException {
        kafkaWriter.send("order", new Order("Product1", 1));

        await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
            assertThat(orderListener.repository).as("Number of attempts to process")
                .hasSize(1));
    }

    @Test
    void validRequestWithString() throws JsonProcessingException {
        kafkaWriter.send("order", "{\"product\": \"Order1\", \"quantity\": 1}");

        await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
            assertThat(orderListener.repository).as("Number of attempts to process")
                .hasSize(1));
    }

    @Test
    void invalidJsonFormat() throws JsonProcessingException {
        kafkaWriter.send("order", "{\"invalid-json {");

        await().pollDelay(Duration.ofSeconds(2)).untilAsserted(() ->
            assertThat(orderListener.repository).as("Number of attempts to process")
                .isEmpty());
    }

    @Test
    void invalidFieldType() throws JsonProcessingException {
        kafkaWriter.send("order", "{\"product\": \"Order1\", \"quantity\": \"not-a-number\"}");

        await().pollDelay(Duration.ofSeconds(2)).untilAsserted(() ->
            assertThat(orderListener.repository).as("Number of attempts to process")
                .isEmpty());
    }

    @Test
    void invalidFieldValue() throws JsonProcessingException {
        kafkaWriter.send("order", new Order(null, 1));

        await().pollDelay(Duration.ofSeconds(2)).untilAsserted(() ->
            assertThat(orderListener.repository).as("Number of attempts to process")
                .isEmpty());
    }

}