package com.wordpress.simplydistributed.kafka.scale;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Bootstrap {

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || args.length > 1) {
            throw new IllegalArgumentException("Provide arguments. Options\n (a)producer\n (b)consumer");
        }
        ExecutorService es = Executors.newFixedThreadPool(1);

        KafkaConsumerTest consumer = null;
        KafkaProducerTest producer = null;

        if (args[0].equals("producer")) {
            producer = new KafkaProducerTest();
            es.execute(producer);
        } else if (args[0].equals("consumer")) {
            consumer = new KafkaConsumerTest();
            es.execute(consumer);

        } else {
            throw new IllegalArgumentException("Provide valid arguments. Options\n (a)producer\n (b)consumer");
        }

        es.shutdown();
    }
}
