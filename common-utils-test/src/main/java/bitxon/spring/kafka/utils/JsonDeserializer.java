package bitxon.spring.kafka.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Arrays;

class JsonDeserializer<T> implements Deserializer<T> {
    private final Class<T> klass;
    private final ObjectMapper jsonMapper;

    public JsonDeserializer(Class<T> klass) {
        this.klass = klass;
        this.jsonMapper = JsonMapper.builder()
            .findAndAddModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (String.class.isAssignableFrom(klass)) {
            return (T) new String(data); // we don't need jsonMapper to convert byte[] to String
        }
        try {
            return jsonMapper.readValue(data, klass);
        } catch (IOException exception) {
            throw new SerializationException(
                "Can't deserialize data [%s] from topic [%s]".formatted(Arrays.toString(data), topic), exception);
        }
    }
}
