package com.kafka.course;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());
    public static void main(String[] args) {
        LOG.info("I am a kafka Consumer");
        String groupId="my-java-application";
        String topic ="demo_java";
        //create Consumer properties

        Properties properties = new Properties();

        //connect to local host
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id",groupId);
        properties.setProperty("auto.offset.reset","earliest");  //latest-(new msg),earliest - from begin


        //create the consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                LOG.info("detected shutdown, let's exit by calling consumer wakeup");
                consumer.wakeup();

                //join the main thread ro allow execution in main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            //subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            //poll for data
            while (true) {
                LOG.info("Polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord record : records) {
                    LOG.info("Key: " + record.key() + " Value: " + record.value());
                    LOG.info("Partition: " + record.partition() + " ,Offset: " + record.value());
                }
            }
        }
        catch (WakeupException e){
            LOG.error("Consumer is staring tp shutdown");
        }
        catch (Exception e){
            LOG.error("Unexpected Exception to consumer "+e);
        }
        finally {
            consumer.close(); //close the consumer, this will also commit the offset
        }


    }
}
