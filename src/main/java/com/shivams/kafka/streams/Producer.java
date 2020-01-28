package com.shivams.kafka.streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {
    final KafkaProducer<String, String> mProducer;
    final Logger mLogger = LoggerFactory.getLogger(Producer.class);

    private Properties setProducerProperties (String bootstrapServer) {
        String serializer = StringSerializer.class.getName();
        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);

        return p;
    }
    Producer(String bootstrapServer) {
        Properties p = setProducerProperties(bootstrapServer);
        mProducer = new KafkaProducer<>(p);
        mLogger.info("Producer Initialized!");
    }
    public void put(String topic, String key, String value) throws ExecutionException, InterruptedException {
        mLogger.info("Put value: " + value + "for key: " + key);
        ProducerRecord<String, String> pRecord = new ProducerRecord<>(topic, key, value);
        mProducer.send(pRecord, (pRecordMetadata, e) ->{
            if(e != null){
                mLogger.info("Error while sending/producing", e);
                return;
            }
            mLogger.info("Received new meta: \n" +
                    "Topic: " + pRecordMetadata.topic() + "\n" +
                    "Partition: " + pRecordMetadata.partition() + "\n" +
                    "Offset: " + pRecordMetadata.offset() + "\n" +
                    "TimeStamp: " + pRecordMetadata.timestamp());
        }).get();
    }
    public void close() {
        mLogger.info("Closing producer's connection");
        mProducer.close();
    }
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String server = "127.0.0.1:56742";
        String topic = "user_registered";

        Producer producer = new Producer(server);
        producer.put(topic, "user1", "Shivam");
        producer.put(topic, "user2", "Anurag");
        producer.close();
        //Properties config = new Properties();
    }
}
