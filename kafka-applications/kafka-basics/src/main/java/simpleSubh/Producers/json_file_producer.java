package simpleSubh.Producers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import simpleSubh.JsonNodeSerde;

import java.io.FileReader;
import java.util.Properties;

public class json_file_producer {
    json_file_producer() {}

    public static void main(String[] args) {
        json_file_producer.run();
    }
    public static void run() {

        KafkaProducer<String, JsonNode> producer = createKafkaProducer();
        String topic1 = "monitor-logs";
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            JsonNode arr = objectMapper.readTree(new FileReader("./kafka-basics/monitor.json"));
            for (int i=0;i<arr.size();i++) {
                JsonNode jsonNode = arr.get(i);

                ProducerRecord<String, JsonNode> record =
                        new ProducerRecord<>(topic1, jsonNode);
                producer.send(record);
            }
            producer.flush();
            producer.close();
        }catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    public static KafkaProducer<String, JsonNode> createKafkaProducer() {
        //Producer Properties
        String bootstrapServer = "pkc-41p56.asia-south1.gcp.confluent.cloud:9092";
        String bootstrap = "pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092";
        String key = "IZCOWX4AGEOYWRWQ", secret = "xlSXy1TYKYsMZLEQEvp7efFLd6JwqxiTHftjJ61MJ2kHoImJqAsqVIN4U7aDQAI2";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonNodeSerde.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config",String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';",key,secret));
        properties.setProperty("sasl.mechanism","PLAIN");

        //return KafkaProducer

        return new KafkaProducer<>(properties);
    }
}