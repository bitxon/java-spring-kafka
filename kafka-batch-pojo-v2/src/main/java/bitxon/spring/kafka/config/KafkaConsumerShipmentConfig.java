package bitxon.spring.kafka.config;

import bitxon.spring.kafka.model.Shipment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;

import java.util.Map;

@Configuration
public class KafkaConsumerShipmentConfig {
    @Value("${custom.shipment.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${custom.shipment.group-id}")
    private String groupId;

    @Bean
    public Map<String, Object> shipmentConsumerProps() {
        return Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG, groupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest", // default='latest'
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10, // default=500

            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );
    }

    @Bean
    public ConsumerFactory<String, Shipment> shipmentConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(shipmentConsumerProps());
    }

    @Bean
    public RecordMessageConverter converter() {
        return new JsonMessageConverter();
    }

    @Bean
    public BatchMessagingMessageConverter batchConverter() {
        return new BatchMessagingMessageConverter(converter());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Shipment> shipmentKafkaListenerContainerFactory(
        CommonErrorHandler commonErrorHandler
    ) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, Shipment>();
        factory.setConsumerFactory(shipmentConsumerFactory());
        factory.setCommonErrorHandler(commonErrorHandler);
        factory.setBatchMessageConverter(batchConverter()); // enables KafkaHeaders.CONVERSION_FAILURES
        return factory;
    }



}
