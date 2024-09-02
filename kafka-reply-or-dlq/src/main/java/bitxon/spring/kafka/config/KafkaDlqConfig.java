package bitxon.spring.kafka.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

import java.util.Map;

@Configuration
public class KafkaDlqConfig {

    /**
     * This will create DLQ Recoverer and will use byteKafkaTemplate for messages that fails to deserializer
     *
     * @see <a href="https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html#dead-letters">dead-letters</a>
     */
    @Bean
    public DeadLetterPublishingRecoverer dlqRecoverer(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                                                      KafkaTemplate<String, Object> kafkaTemplate) {
        var byteKafkaTemplate = bytesKafkaTemplate(bootstrapServers);

        return new DeadLetterPublishingRecoverer(
            (rec) -> resolveTemplate(rec, kafkaTemplate, byteKafkaTemplate),
            (rec, ex) -> new TopicPartition("invoice-dlq", rec.partition())
        );
    }

    private static KafkaTemplate<?, ?> resolveTemplate(ProducerRecord<?, ?> rec,
                                                       KafkaTemplate<?, ?> defaultTemplate,
                                                       KafkaTemplate<String, byte[]> byteTemplate) {
        var value = rec.value();
        if (value == null) {
            return defaultTemplate;
        }
        if (byte[].class.isAssignableFrom(value.getClass())) {
            return byteTemplate; // publish byte[] that were NOT deserialized
        }
        return defaultTemplate; // publish records that were deserialized successfully
    }


    private static KafkaTemplate<String, byte[]> bytesKafkaTemplate(String bootstrapServers) {
        Map<String, Object> config = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class
        );
        var producerFactory = new DefaultKafkaProducerFactory<String, byte[]>(config);

        return new KafkaTemplate<>(producerFactory);
    }
}
