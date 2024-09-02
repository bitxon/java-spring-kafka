package bitxon.spring.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@TestConfiguration(proxyBeanMethods = false)
public class TestContainersConfig {

    @Bean
    @ServiceConnection
    public KafkaContainer kafkaContainer(DynamicPropertyRegistry dynamicPropertyRegistry) {
        var container = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.1"));
        dynamicPropertyRegistry.add("spring.kafka.bootstrap-servers", container::getBootstrapServers);
        return container;
    }

    @Bean
    public NewTopic invoiceRequest() {
        return new NewTopic("invoice-request", 1, (short) 1);
    }

    @Bean
    public NewTopic invoiceResponse() {
        return new NewTopic("invoice-response", 1, (short) 1);
    }

    @Bean
    public NewTopic invoiceDlq() {
        return new NewTopic("invoice-dlq", 1, (short) 1);
    }
}
