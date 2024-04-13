package simpleSubh.Producers;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


public class avro_file_producer {

    avro_file_producer() {}

    public static void main(String[] args) throws IOException, InterruptedException, RestClientException {
        avro_file_producer.run();
    }
    public static void run() throws IOException, RestClientException {

        String topic = "sourceTopic";

        // schema registry url.
        String url = "http://broker1:8081";

        // avro schema avsc file path.
        String schemaPath = "/rental_schema.avsc";

        // subject convention is "<topic-name>-value"
        String subject = topic + "-value";

        // avsc json string.
        String schema = null;

        FileInputStream inputStream = new FileInputStream(schemaPath);
        try {
            schema = inputStream.toString();
        } finally {
            inputStream.close();
        }

        Schema avroSchema = new Schema.Parser().parse(schema);
        ParsedSchema parsedSchema = new AvroSchema(schema);
        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(url,20);

        client.register(subject, parsedSchema);

        KafkaProducer<String, GenericRecord> producer = createKafkaProducer();

        int rentalID = 1;


    }
    public static KafkaProducer<String, GenericRecord> createKafkaProducer() {
        //Producer Properties
        String bootstrapServer = "pkc-41p56.asia-south1.gcp.confluent.cloud:9092";
        String registry = "http://broker1:8081";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,registry);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='HOATM2UD5BLDQ7GZ' password='HKxAUO0+AJpMYx5omxfuOMdJVpQhjDMOJglPozznyEtZW0VhYeY8owLxY9mUVsW7';");
        properties.setProperty("sasl.mechanism","PLAIN");

        //return KafkaProducer

        return new KafkaProducer<>(properties);
    }

    public GenericRecord buildRecord() throws IOException {

        String schemaPath = "/rental_schema.avsc";

        // avsc json string.
        String schema = null;

        FileInputStream inputStream = new FileInputStream(schemaPath);
        try {
            schema = inputStream.toString();
        } finally {
            inputStream.close();
        }

        Schema avroSchema = new Schema.Parser().parse(schema);

        GenericData.Record record = new GenericData.Record(avroSchema);

        record.put("RentalTourVoucherId","9");
        record.put("RentalTransactionType","null");
        return record;
    }
}

//    JSONParser parser = new JSONParser();
//        try {
//            JSONArray arr = (JSONArray) parser.parse(new FileReader("C:/Users/subha/Downloads/airplane.json"));
//
//            for (Object o : arr) {
//                JSONObject jsonObject = (JSONObject) o;
//
//                String key = (String) jsonObject.get("airplane_id");
//
//                ProducerRecord<String, String> record =
//                        new ProducerRecord<>(topic, key, jsonObject.toString());
//                producer.send(record);
//                Thread.sleep(100);
//            }
//
//            producer.flush();
//            producer.close();
//        }catch (Exception e) {
//            System.out.println(e.getMessage());
//        }