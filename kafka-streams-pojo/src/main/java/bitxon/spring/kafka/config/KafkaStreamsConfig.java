package bitxon.spring.kafka.config;

import bitxon.spring.kafka.model.Invoice;
import bitxon.spring.kafka.model.InvoiceProcessed;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;


@Slf4j
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {
    private static final String INPUT_TOPIC = "streams-input";
    private static final String OUTPUT_TOPIC = "streams-output";

    @Bean
    public KStream<String, Invoice> invoiceKafkaStreams(StreamsBuilder builder) {
        KStream<String, Invoice> stream = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), new JsonSerde<>(Invoice.class)));

        stream.foreach((key, value) -> log.info("Received message: {}={}", key, value));
        stream.mapValues(val -> new InvoiceProcessed(val.id(), val.message()))
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(InvoiceProcessed.class)));

        return stream;
    }

}
