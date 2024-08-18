package bitxon.spring.kafka.config;

import bitxon.spring.kafka.utils.KafkaWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@TestConfiguration(proxyBeanMethods = false)
public class TestProducerConfig {

    @Bean
    public KafkaWriter kafkaWriter(@Value("${custom.shipment.bootstrap-servers}") String bootstrapServers) {
        return new KafkaWriter(bootstrapServers, KafkaWriter.Mode.BATCH);
    }
}
