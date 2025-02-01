package bitxon.spring.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@TestConfiguration(proxyBeanMethods = false)
public class TestContainersConfig {

    @Bean
    public DynamicPropertyRegistrar propertiesOverride(KafkaContainer kafka) {
        return (registry) -> registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Bean
    @ServiceConnection
    public KafkaContainer kafkaContainer() {
        return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.1"));
    }

    @Bean
    public NewTopic input() {
        return new NewTopic("streams-input", 1, (short) 1);
    }

    @Bean
    public NewTopic output() {
        return new NewTopic("streams-output", 1, (short) 1);
    }

}
