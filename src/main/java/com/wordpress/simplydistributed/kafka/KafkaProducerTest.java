package com.wordpress.simplydistributed.kafka;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducerTest {

    private static final Logger LOGGER = Logger.getLogger(KafkaProducerTest.class.getName());
    private static final String TOPIC_NAME = "a-topic";

    public static void produce() throws InterruptedException, ExecutionException, TimeoutException {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "1");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProps);
        ProducerRecord<String, String> record = null;

        while (true) {
            String key = "key-" + UUID.randomUUID().toString();
            String value = "value-" + UUID.randomUUID().toString();
            record = new ProducerRecord<>(TOPIC_NAME, key, value);

            kafkaProducer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata rm, Exception excptn) {
                    if (excptn != null) {
                        LOGGER.log(Level.WARNING, "Error sending message with key {0}\n{1}", new Object[]{key, excptn.getMessage()});
                    } else {
                        LOGGER.log(Level.INFO, "Partition for key {0} is {1}", new Object[]{key, rm.partition()});
                    }

                }
            });

            Thread.sleep(1000); //wait before sending next message
        }

    }

}
