package com.simple.project.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.logging.Logger;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        Logger logger = Logger.getLogger(ProducerDemoWithCallback.class.getName());
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer(properties);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello from java " + i);

            producer.send(record, (metadata, e) -> {
                if (e == null) {
                    logger.info("success");
                    logger.info("topic: " + metadata.topic());
                    logger.info("partition: " + metadata.partition());
                    logger.info("offset: " + metadata.offset());
                    logger.info("timestamp: " + metadata.timestamp());

                } else {
                    logger.severe("error: " + e);
                }
            });
        }

        producer.close();
    }
}
