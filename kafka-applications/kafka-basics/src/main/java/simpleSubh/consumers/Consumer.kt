package simpleSubh.consumers

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import kotlin.concurrent.thread

val log: Logger = LoggerFactory.getLogger("main")

fun main() {
    run()
}

fun run(){
    val consumer = createKafkaConsumer()
    val topic = "test-topic"
    val mainThread = Thread.currentThread()

    // adding shutdown hook
    Runtime.getRuntime().addShutdownHook(thread(start = false) {
        log.info("Detected a shutdown, consumer shutting down...")
        consumer.wakeup()

        // join the main thread to allow the execution of the code in the main thread
        try {
            mainThread.join()
        } catch (e: InterruptedException) {
            e.printStackTrace()
        }
    })

    try {
        consumer.subscribe(Collections.singleton(topic))

        while (true) {
            val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofMillis(100))
            records.forEach {
                println(
                    """Received new data. 
                    Key : ${it.key()}
                    Value : ${it.value()}
                    Partition : ${it.partition()}
                    Offset :${it.offset()}
                    """
                )
            }
        }
    } catch (e: WakeupException) {
        log.info("Wakeup exception")
    } catch (e: Exception){
        log.info("Unexpected exception")
    } finally {
        consumer.close()
        log.info("Consumer is closed..")
    }

}

fun createKafkaConsumer(): KafkaConsumer<String,String> {
    val cluster_1 = "pkc-41p56.asia-south1.gcp.confluent.cloud:9092"
    val key_1 = "4ELWO4KWM7S4GVXB"
    val secret_1 = "ThNTRzw8xxx2EpFqnbiLjS8HPv44SaHBtm9CFsVPvqHbzGGPexjqZ2EIy/w08A8k"
    val groupId = "kotlin-Consumer-2"
    
    val properties = Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster_1)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.setProperty("security.protocol", "SASL_SSL")
    properties.setProperty(
        "sasl.jaas.config",
        String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';",
            key_1,
            secret_1
        )
    )
    properties.setProperty("sasl.mechanism", "PLAIN")


    return KafkaConsumer(properties)
}