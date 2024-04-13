package simpleSubh;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import java.io.IOException;
import java.util.Map;

public class JsonNodeSerde<T extends JsonNode> implements Serializer<T>, Deserializer<T>, Serde<T> {

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String s, T jsonNodes) {

        ObjectMapper mapper = new ObjectMapper();

        try {
            return mapper.writeValueAsBytes(jsonNodes);
        }catch (Exception ex){
            ex.printStackTrace();
        }

        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(String s, byte[] data) {

        ObjectMapper mapper = new ObjectMapper();

        try {
            return (T) mapper.readValue(data, JsonNode.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void close() {
        Serializer.super.close();
        Deserializer.super.close();
    }
}
