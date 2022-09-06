package com.simple.project.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = Logger.getLogger(ProducerDemoWithKeys.class.getName());
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer(properties);

        for (int i = 0; i < 10; i++) {
            String topicName = "first_topic";
            String value = "hello from java " + i;
            String key = "id_" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

            logger.info(key);
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
            }).get();
        }

        producer.close();
    }
}
