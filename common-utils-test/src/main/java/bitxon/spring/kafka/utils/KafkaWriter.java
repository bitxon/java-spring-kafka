package bitxon.spring.kafka.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collection;
import java.util.Map;

public class KafkaWriter {
    private final KafkaProducer<String, Object> producer;
    private final String topic;

    public KafkaWriter(String bootstrapServers, String topic) {
        Map<String, Object> config = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ProducerConfig.ACKS_CONFIG, "all",  //default=all
            ProducerConfig.LINGER_MS_CONFIG, "100"  //default=0, Increase value to make sure we send batch
        );

        this.producer = new KafkaProducer<>(
            config,
            new StringSerializer(),
            new JsonSerializer<>()
        );

         this.topic = topic;
    }

    public void send(Object value) {
        send(value, null);
    }

    public void send(Collection<Object> values) {
        for (Object rawPayload : values) {
            send(rawPayload, null);
        }
    }

    public void send(Object value, Map<String, String> rawHeaders) {
        final var headers = rawHeaders == null ? null : rawHeaders.entrySet().stream()
            .map(e -> (Header) new RecordHeader(e.getKey(), e.getValue().getBytes()))
            .toList();

        var record = new ProducerRecord<String, Object>(topic, null, null, null, value, headers);

        producer.send(record);
        // NOTE: producer.flush() - will ignore 'linger.ms'
    }

    public void flush() {
        producer.flush();
    }
}
