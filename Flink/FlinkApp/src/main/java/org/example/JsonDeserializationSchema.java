package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class JsonDeserializationSchema<T> implements DeserializationSchema<T> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final Class<T> clazz;

    public JsonDeserializationSchema(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, clazz);
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(clazz);
    }
}

