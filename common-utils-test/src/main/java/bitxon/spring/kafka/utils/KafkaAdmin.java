package bitxon.spring.kafka.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class KafkaAdmin {
    private final AdminClient adminClient;
    private final String topic;
    private final String consumerGroupId;

    public KafkaAdmin(String bootstrapServers, String topic, String consumerGroupId) {
        Map<String, Object> config = Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
        );

        this.adminClient = AdminClient.create(config);
        this.topic = topic;
        this.consumerGroupId = consumerGroupId;
    }

    public Set<String> consumerIds() {
        try {
            return adminClient.listConsumerGroups().all().get().stream()
                .map(ConsumerGroupListing::groupId)
                .collect(Collectors.toSet());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<Integer> partitions() {
        try {
            return adminClient.describeTopics(List.of(topic))
                .allTopicNames().get().get(topic)
                .partitions().stream()
                .map(TopicPartitionInfo::partition)
                .collect(Collectors.toSet());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<Integer, Long> offsetsLag() {
        var latestOffsets = offsetsLatest();
        var currentOffsets = offsetsCurrent();
        return latestOffsets.entrySet().stream().collect(Collectors.toMap(
            e -> e.getKey(),
            e -> e.getValue() - currentOffsets.getOrDefault(e.getKey(), 0L) //TODO count earliest
        ));
    }

    public Map<Integer, Long> offsetsCurrent() {
        try {
            var validGroupIds = consumerIds();
            if (!validGroupIds.contains(consumerGroupId)) {
                throw new IllegalArgumentException("Consumer group '%s' not found in list '%s'".formatted(consumerGroupId, validGroupIds));
            }

            return adminClient.listConsumerGroupOffsets(consumerGroupId)
                .partitionsToOffsetAndMetadata()
                .get()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                    e -> e.getKey().partition(),
                    e -> e.getValue().offset()
                ));
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<Integer, Long> offsetsLatest() {
        try {
            var requestLatestForAllPartitions = partitions().stream().collect(Collectors.toMap(
                partition -> new TopicPartition(topic, partition),
                partition -> OffsetSpec.latest()
            ));

            return adminClient.listOffsets(requestLatestForAllPartitions)
                .all()
                .get()
                .entrySet().stream()
                .collect(Collectors.toMap(
                    e -> e.getKey().partition(),
                    e -> e.getValue().offset()
                ));

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}
