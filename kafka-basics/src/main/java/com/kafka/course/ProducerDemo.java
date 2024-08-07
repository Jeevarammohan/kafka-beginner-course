package com.kafka.course;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        LOG.info("I am a kafka Producer");
        //create Producer properties

        Properties properties = new Properties();

        //connect to local host
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        //create a producer record
        ProducerRecord<String,String> record = new ProducerRecord<>("demo_java","Hello Jeeva");

        //send data
        producer.send(record);

        //tell the producer to send all the data and block until done - synchronous
        producer.flush();

        //flush and close the producer
        producer.close();

    }
}
