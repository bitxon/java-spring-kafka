package bitxon.spring.kafka.config;

import bitxon.spring.kafka.model.Shipment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.JsonMessageConverter;

import java.util.Map;

@Configuration
public class KafkaConsumerShipmentConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Bean
    public Map<String, Object> shipmentConsumerProps() {
        return Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG, groupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",

            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Shipment> shipmentKafkaListenerContainerFactory(
        CommonErrorHandler commonErrorHandler
    ) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, Shipment>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(shipmentConsumerProps()));
        factory.setCommonErrorHandler(commonErrorHandler);
        // Following line is paramount for having KafkaHeaders.CONVERSION_FAILURES
        factory.setBatchMessageConverter(new BatchMessagingMessageConverter(new JsonMessageConverter()));
        return factory;
    }


//    @Bean
//    public RecordMessageConverter converter() {
//        return new JsonMessageConverter();
//    }
//
//    @Bean
//    public BatchMessagingMessageConverter batchConverter() {
//        return new BatchMessagingMessageConverter(converter());
//    }
}
