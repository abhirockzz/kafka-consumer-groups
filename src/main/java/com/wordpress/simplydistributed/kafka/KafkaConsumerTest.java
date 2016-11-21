package com.wordpress.simplydistributed.kafka;

import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerTest {

    private static final Logger LOGGER = Logger.getLogger(KafkaConsumerTest.class.getName());
    private static final String TOPIC_NAME = "a-topic";
    private static final String CONSUMER_GROUP = "a-group";

    public static void consume() {
        
        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        
        LOGGER.log(Level.INFO, "Subcribed to: {0}", TOPIC_NAME);
        
        while (true) {
            ConsumerRecords<String, String> msg = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : msg) {
                LOGGER.log(Level.INFO, "Key: {0}", record.key());
                LOGGER.log(Level.INFO, "Value: {0}", record.value());
                LOGGER.log(Level.INFO, "Partition: {0}", record.partition());
                LOGGER.log(Level.INFO, "---------------------------------------");
            }

        }

    }

}
