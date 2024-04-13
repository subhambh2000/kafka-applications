package simpleSubh.Producers;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.json.XML;
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

//        Scanner sc = new Scanner(System.in);
        KafkaProducer<String, String> producer = createKafkaProducer();
        String topic1 = "test_json";

        try {
            int flag;
            do {
                byte[] b=hexatobytearray("1f8b08000000000000009d955d939a301486ff4a26b71d85840f811177d8d2edba6b570769f7b293815499416092a0edbf6f1014a8ba1dbcc9243979df73ce6382d387dfbb14ec29e3499eb9108d55086816e571926d5cf83d7c1a59107041b298a479465df88772f8309bae8a9c0920b51977e15688c25194c3e130165b9252be6179598ca37ca73021126555f2ed4a9e57f6c884b5c8c9381e205c475b1a97f280b2d75a07ed3e07dc3ae8031c9e72b6234262ea5b18c32c6844b8e8b761de59046a2d26434888a3c137ca39d9d0be8f35c0276424c9bc9432d1f7b0877a2c594c59cf02a9433c7c9f08d2d7a3017a2f256cd76f01b577f35f6d1086f3b3b642597285e5b9f829e542be05ac626da49a2364840839aae1a8786ce948b755f3932a37e4fb6a5f9b299fdb6c5a06a02c6222e892259b44eefb841d924c46c23560495c9baa2632262646c8362028abdd45b3e03ceee6953a792b9d451e1d7f67208ad485dec20fd6cf2104074164625988e6686ab58ceba57e5c16a7a85ecd4fa1c69130068868d4504e3fa784cb96bd52e4d5a58c64292c7261e843a556c4b400b451184db469ae3951a444806a38c63c08a23cfbe542c14a0a6768aa9ccecceae9a9a99b2dbebc553d15754ab3eaa9c9232b3d9732b928e5ffe6ebe71fdee24b879fd5816777c8591d727687dc29b975954397947d93d44022abe03198872f2d11a45d2782f4e1441e83d7f7e5d26f8920a377a3aa4ffd094a3d3fef5f40a9e21f43b971e20e28efcbd7f9dbd72e14fb3a14ac0e87529bb74cb0da638271ef9561d462c1f8124b15ff188b54dd854509d7722803391cffc9677f01c677ae6708080000");
                InputStream stream = new GZIPInputStream(new ByteArrayInputStream(b));
                Scanner sc = new Scanner(stream).useDelimiter("\\A");
                String str = sc.hasNext() ? sc.next() : "";

                JSONObject data_json = XML.toJSONObject(str);

                ProducerRecord<String, String> record = new ProducerRecord<>(topic1, data_json.toString());
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