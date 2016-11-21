package com.wordpress.simplydistributed.kafka;


public class Bootstrap {
    public static void main(String[] args) throws Exception {
        if(args.length == 0 || args.length > 1){
            throw new IllegalArgumentException("Provide arguments. Options\n (a)producer\n (b)consumer");
        }
        
        if(args[0].equals("producer")){
            KafkaProducerTest.produce();
        }else if(args[0].equals("consumer")){
            KafkaConsumerTest.consume();
        }else {
            throw new IllegalArgumentException("Provide valid arguments. Options\n (a)producer\n (b)consumer");
        }
    }
}
