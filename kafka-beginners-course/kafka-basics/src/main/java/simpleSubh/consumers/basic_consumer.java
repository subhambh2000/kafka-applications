package simpleSubh.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class basic_consumer {

    public static void main(String[] args) {
        basic_consumer.run();
    }
    public static void run(){
        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        Logger log = LoggerFactory.getLogger(basic_consumer.class);
        String topic = "json-data";
        final Thread main = Thread.currentThread();
        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, consumer shutting down...");
            consumer.wakeup();

            // join the main thread to allow the execution of the code in the main thread
            try {
                main.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try
        {
            consumer.subscribe(Collections.singletonList(topic)); // change to Arrays.asList if passing string array of topics instead a single string

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Received new data. \n" +
                            "Key : " + record.key() + "\n" +
                            "Value : " + record.value() + "\n" +
                            "Partition : " + record.partition() + "\n" +
                            "Offset :" + record.offset());
                }

            }
        } catch (WakeupException e) {
            log.info("Wake up exception!");
            // we ignore this as this is an expected exception when closing a consumer
        } catch (Exception e){
            log.error("Unexpected exception",e);
        }
        finally {
            consumer.close();
            log.info("Consumer is closed..");
        }
    }
    public static KafkaConsumer<String,String> createKafkaConsumer(){
        String cluster_1 = "pkc-41p56.asia-south1.gcp.confluent.cloud:9092";
        String key_1 = "4ELWO4KWM7S4GVXB", secret_1 = "ThNTRzw8xxx2EpFqnbiLjS8HPv44SaHBtm9CFsVPvqHbzGGPexjqZ2EIy/w08A8k";
        String groupId = "basic-Consumer-10";

        //consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,cluster_1);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config",String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';",key_1,secret_1));
        properties.setProperty("sasl.mechanism","PLAIN");

        //return KafkaConsumer
        return new KafkaConsumer<>(properties);
    }
}