package bitxon.spring.kafka.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

class JsonSerializer<T> implements Serializer<T> {
    private final ObjectMapper jsonMapper;

    public JsonSerializer() {
        jsonMapper = JsonMapper.builder()
            .findAndAddModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        if (data instanceof String string) { // assume String is already a Json
            return string.getBytes();
        }
        try {
            return jsonMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException exception) {
            throw new SerializationException(
                "Can't serialize data [%s] for topic [%s]".formatted(data, topic), exception);
        }

    }
}
