package bitxon.spring.kafka.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collection;
import java.util.Map;

public class KafkaWriter {

    private final ObjectMapper jsonMapper;
    private final KafkaProducer<String, String> producer;

    public KafkaWriter(String bootstrapServers) {
        this(bootstrapServers, Mode.DEFAULT);
    }

    public KafkaWriter(String bootstrapServers, Mode mode) {
        Map<String, Object> config = switch (mode) {
            case DEFAULT -> Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.ACKS_CONFIG, "all"   //default=all
            );
            case BATCH -> Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.ACKS_CONFIG, "all",  //default=all
                ProducerConfig.LINGER_MS_CONFIG, "100"  //default=0, Increase value to make sure we send batch
            );
        };

        producer = new KafkaProducer<>(
            config,
            new StringSerializer(),
            new StringSerializer()
        );

        jsonMapper = JsonMapper.builder()
            .findAndAddModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();
    }

    public void send(String topic, Object rawPayload) throws JsonProcessingException {
        send(topic, rawPayload, null);
    }

    public void send(String topic, Collection<Object> rawPayloads) throws JsonProcessingException {
        for (Object rawPayload : rawPayloads) {
            send(topic, rawPayload);
        }
    }

    public void send(String topic, Object rawPayload, Map<String, String> rawHeaders) throws JsonProcessingException {
        final String payload;
        if (rawPayload == null) {
            payload = null;
        } else if (rawPayload instanceof String string) {
            payload = string;
        } else {
            payload = jsonMapper.writeValueAsString(rawPayload);
        }

        final var headers = rawHeaders == null ? null : rawHeaders.entrySet().stream()
            .map(e -> (Header) new RecordHeader(e.getKey(), e.getValue().getBytes()))
            .toList();

        var record = new ProducerRecord<String, String>(topic, null, null, null, payload, headers);
        producer.send(record);
    }

    public enum Mode {
        DEFAULT,
        BATCH
    }
}
