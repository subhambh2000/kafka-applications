package simpleSubh.Producers;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

public class basic_producer {
    basic_producer() {}

    public static void main(String[] args) throws IOException, InterruptedException {
        basic_producer.run();
    }
    public static void run() throws IOException, InterruptedException {

        Scanner sc = new Scanner(System.in);
        int flag=1;
        KafkaProducer<String, String> producer = createKafkaProducer();
        String topic1 = "test_json";

        try {
            int flag;
            do {
                // Scanner sc1 = new Scanner(System.in);
                String text = sc.nextLine();
                ProducerRecord<String, String> record = new ProducerRecord<>(topic1, text);
                // sending data
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        Logger log = LoggerFactory.getLogger(basic_producer.class);
                        if (e == null){
                            log.info("Received new metadata. \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Key: " + record.key() + "\n" +
                                    "Value: " + record.value() + "\n");
                        }
                        else {
                            log.error("Error while producing", e);
                        }
                    }
                }).get(); //sending synchronous data forcefully
                // wihtout get asynchronous data will be sent
                System.out.println("Enter Again: ");
                flag = sc.nextInt();
                sc.nextLine();
            }while (flag != 0);

            producer.flush();
            producer.close();
        }catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static byte[] hexatobytearray(String s){
        byte[] ans = new byte[s.length() / 2];

        System.out.println("Hex String : "+s);

        for (int i = 0; i < ans.length; i++) {
            int index = i * 2;

            // Using parseInt() method of Integer class
            int val = Integer.parseInt(s.substring(index, index + 2), 16);
            ans[i] = (byte)val;
        }
        return ans;
    }
    public static KafkaProducer<String, String> createKafkaProducer() {
        //Producer Properties
        String bootstrap_server = "pkc-41p56.asia-south1.gcp.confluent.cloud:9092";
        //String bootstrap_dedicated = "pkc-g5znm.asia-south2.gcp.confluent.cloud:9092";
//        String localhost = "172.26.196.166:9092"; //ip of wsl (command to retrieve the ip - [ip addr | grep "eth0"] )
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='4ELWO4KWM7S4GVXB' password='ThNTRzw8xxx2EpFqnbiLjS8HPv44SaHBtm9CFsVPvqHbzGGPexjqZ2EIy/w08A8k';");
        properties.setProperty("sasl.mechanism","PLAIN");

        //return KafkaProducer

        return new KafkaProducer<>(properties);
    }
}
