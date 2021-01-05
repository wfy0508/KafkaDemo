package com.wfy.kafka.consumer;
/*
    PROJECT_NAME: KafkaDemo
    User: Summer
    Create time: 2021/1/5 17:01
*/

import org.apache.kafka.clients.consumer.*;

import java.util.Arrays;
import java.util.Properties;

// 同步提交
public class commitSync {
    public static void main(String[] args) {
        // 1. 创建消费者配置信息
        Properties props = new Properties();
        // Kafka集群
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.168.10:9092");
        // 反序列化
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdata");
        // 重置消费者的offset（只有在换了新的消费者组或者没有更换消费者组但是保存的offset已经失效时，配置才会生效）
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // 2. 创建消费者
        Consumer<String, String> consumer = new KafkaConsumer<>(props);

        // 3. 订阅主题(可以订阅多个主题)
        consumer.subscribe(Arrays.asList("first", "second"));

        // Ctrl+C结束
        while (true) {
            // 4. 获取数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);

            // 5. 解析并打印consumerRecords
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println(record.key() + "--" + record.value());
            }

            // 同步提交，当前线程会阻塞，直到offset提交成功（会比较慢）
            consumer.commitSync();

        }
    }
}
