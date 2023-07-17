package org.example.kafkatool;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaConsumerHolder extends ClosableHolder{
    private static volatile KafkaConsumer<String, String> kafkaConsumer = null;

    private KafkaConsumerHolder() {

    }

    public static KafkaConsumer<String, String> getConsumer(String host, Integer port, String groupId,ClosableHolder closableHolder) {
        if (kafkaConsumer == null) {
            synchronized (KafkaConsumerHolder.class) {
                if (kafkaConsumer == null) {
                    Properties properties = new Properties();
                    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":" + port);
                    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                    if (groupId != null) {
                        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                    }
                    kafkaConsumer= new KafkaConsumer<>(properties);
                    closableHolder.registerIfNotExists(KafkaConsumerHolder.class.getName(), () -> kafkaConsumer.close());
                }
            }
        }
        return kafkaConsumer;
    }

    public static void close() {
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
        }
    }
}
