package com.wfy.kafka.producer;
/*
    PROJECT_NAME: KafkaDemo
    User: Summer
    Create time: 2021/1/5 15:09
*/

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class PartitionProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.168.10:9092"); // Kafka集群
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3); // 重试次数
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); //批次大小
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1); // 等待时间
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 分区器
        props.put("partitioner.class", "com.wfy.kafka.partitioner.MyPartitioner");

        // 2. 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 3. 发送数据
        for (int i = 0; i <= 10; i++) {
            producer.send(new ProducerRecord<>("first", "wfy","kafka_test_" + i), (metadata, exception) -> {
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
