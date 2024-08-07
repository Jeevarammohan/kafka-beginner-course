package com.kafka.course;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoWithCallBack.class.getSimpleName());
    public static void main(String[] args) {
        LOG.info("I am a kafka Producer");
        //create Producer properties

        KafkaProducer<String, String> producer = getKafkaProducerProperties();
        for(int j=0;j<2;j++){
            for(int index=0;index<10;index++){
                String topic ="demo_java";
                String key = "Id_"+index;
                String value = "Hello Jeeva "+index*j;
                //create a producer record
                ProducerRecord<String,String> record = new ProducerRecord<>(topic,key,value);

                //send data
                producer.send(record, (recordMetadata, e) -> {
                    //executed every time a record successfully sent or an exception is thrown
                    if(e==null){
                        //the record was successful
                        LOG.info(//"Received new Metadata \n"+
                                "Key: "+key+" | Partition: "+recordMetadata.partition()+" \n"
//                                "Offset: "+recordMetadata.offset()+" \n"+
//                                "timestamp: "+recordMetadata.timestamp()
                               );
                    }
                    else{
                        LOG.error("An exception occurred "+e);
                    }
                });
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }  //written for batch feature
            }
        }

        //tell the producer to send all the data and block until done - synchronous
        producer.flush();

        //flush and close the producer
        producer.close();

    }

    private static KafkaProducer<String, String> getKafkaProducerProperties() {
        Properties properties = new Properties();

        //connect to local host
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
//        properties.setProperty("batch.size","400");


        //create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        return producer;
    }
}
