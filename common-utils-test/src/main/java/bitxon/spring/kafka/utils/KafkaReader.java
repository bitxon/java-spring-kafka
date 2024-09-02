package bitxon.spring.kafka.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class KafkaReader<T> {
    private final KafkaConsumer<String, T> consumer;
    private final CopyOnWriteArrayList<T> values;

    public KafkaReader(String bootstrapServers, String topic, Class<T> klass) {
        Map<String, Object> config = Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest", // default='latest'
            ConsumerConfig.GROUP_ID_CONFIG, "test-%s".formatted(topic)
        );

        consumer = new KafkaConsumer<>(
            config,
            new StringDeserializer(),
            new JsonDeserializer<>(klass)
        );
        consumer.subscribe(List.of(topic));

        values = new CopyOnWriteArrayList<>();

        Executors
            .newScheduledThreadPool(1)
            .scheduleAtFixedRate(this::pollNewRecords, 1000, 100, TimeUnit.MILLISECONDS);
    }

    private void pollNewRecords() {
        consumer.poll(Duration.ofMillis(500 /*timeout*/)).forEach(record -> values.add(record.value()));
        consumer.commitSync();
    }

    public List<T> find(Predicate<T> predicate) {
        return values.stream()
            .filter(predicate)
            .toList();
    }

    public void clear() {
        values.clear();
    }

}
