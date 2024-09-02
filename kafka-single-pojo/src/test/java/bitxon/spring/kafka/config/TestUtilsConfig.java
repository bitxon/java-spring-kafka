package bitxon.spring.kafka.config;

import bitxon.spring.kafka.utils.KafkaWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@TestConfiguration(proxyBeanMethods = false)
public class TestUtilsConfig {

    @Bean
    public KafkaWriter paymentKafkaWriter(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        return new KafkaWriter(bootstrapServers, "payment");
    }

    @Bean
    public KafkaWriter orderKafkaWriter(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        return new KafkaWriter(bootstrapServers, "order");
    }
}