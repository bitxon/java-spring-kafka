package bitxon.spring.kafka.config;

import bitxon.spring.kafka.model.InvoiceProcessed;
import bitxon.spring.kafka.utils.KafkaReader;
import bitxon.spring.kafka.utils.KafkaWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@TestConfiguration(proxyBeanMethods = false)
public class TestUtilsConfig {

    @Bean
    public KafkaWriter kafkaWriter(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        return new KafkaWriter(bootstrapServers, "streams-input");
    }

    @Bean
    public KafkaReader<InvoiceProcessed> kafkaReader(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        return new KafkaReader<>(bootstrapServers, "streams-output", InvoiceProcessed.class);
    }
}
