package com.wfy.kafka.consumer;
/*
    PROJECT_NAME: KafkaDemo
    User: Summer
    Create time: 2021/1/5 17:04
*/

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

// 异步提交
public class commitAsync {
    public static void main(String[] args) {
        // 1. 创建消费者配置信息
        Properties props = new Properties();
        // Kafka集群
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.168.10:9092");
        // 开启自动提交
        // 如果设置为false，消费进度在停机后重新开启，offset不会更新，但如果不停机并持续消费，不会有什么影响
        // 自动提交是基于时间的，开发人员难以把握提交的时机
        //props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 自动提交延时
        //props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        // 反序列化
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdata");
        // 重置消费者的offset（只有在换了新的消费者组或者没有更换消费者组但是保存的offset已经失效时，配置才会生效）
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

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

            // 异步提交
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception != null) {
                        System.out.println("Commit failed for" + offsets);
                    }
                }
            });

        }
    }
}
