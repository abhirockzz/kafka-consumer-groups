package com.wordpress.simplydistributed.kafka.scale;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducerTest implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(KafkaProducerTest.class.getName());
    private static final String TOPIC_NAME = "a-topic";
    private KafkaProducer<String, String> kafkaProducer = null;
    private final AtomicBoolean PRODUCER_STOPPED = new AtomicBoolean(false);

    public KafkaProducerTest() {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "1");
        //kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "100"); //commented out on purpose
        
        this.kafkaProducer = new KafkaProducer<>(kafkaProps);

    }

    /**
     * stop the producer
     */
    public void stop() {
        LOGGER.log(Level.INFO, "signalling shut down for producer");
        PRODUCER_STOPPED.set(true);
        LOGGER.log(Level.INFO, "shut down producer");
    }

    @Override
    public void run() {
        try {
            produce();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    /**
     * produce messages
     * 
     * @throws Exception 
     */
    private void produce() throws Exception {

        ProducerRecord<String, String> record = null;

        while (!PRODUCER_STOPPED.get()) {
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
            /**
             * wait before sending next message. this has been done on purpose
             */
            Thread.sleep(1000); 
        }
        LOGGER.log(Level.INFO, "good bye from producer");
    }

}
