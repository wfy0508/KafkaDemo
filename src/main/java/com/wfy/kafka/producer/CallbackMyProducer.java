package com.wfy.kafka.producer;
/*
    PROJECT_NAME: KafkaDemo
    User: Summer
    Create time: 2021/1/5 11:53
*/

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CallbackMyProducer {
    public static void main(String[] args) {
        // 1. 创建Kafka生产者的配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.168.10:9092"); // Kafka集群
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 3); // 重试次数
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); //批次大小
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1); // 等待时间
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 2. 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 3. 发送数据
        for (int i = 0; i <= 10; i++) {
            producer.send(new ProducerRecord<>("first", "kafka_test_" + i), (metadata, exception) -> {
                // exception == null表示消息发送成功，不为null表示发送不成功
                if (exception == null) {
                    System.out.println(metadata.partition() + "--" + metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });
        }
        producer.close();
    }
}
