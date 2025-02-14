package com.kafka.course;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
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

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        //poll for data
        while(true){
            LOG.info("Polling");
           ConsumerRecords<String, String> records= consumer.poll(Duration.ofMillis(1000));
           for(ConsumerRecord record:records){
               LOG.info("Key: "+record.key() +" Value: "+record.value());
               LOG.info("Partition: "+record.partition()+" ,Offset: "+record.value());
           }
        }


    }
}
