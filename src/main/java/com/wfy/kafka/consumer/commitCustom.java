package com.wfy.kafka.consumer;
/*
    PROJECT_NAME: KafkaDemo
    User: Summer
    Create time: 2021/1/5 17:24
*/

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

// 自定义提交
public class commitCustom {
    private static final Map<TopicPartition, Long> currentOffset = new HashMap<>();

    public static void main(String[] args) {
        // 1. 创建消费者配置信息
        Properties props = new Properties();
        // Kafka集群
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.168.10:9092");
        // 关闭自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // 反序列化
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

        // 2. 创建消费者
        Consumer<String, String> consumer = new KafkaConsumer<>(props);

        // 3. 订阅主题(可以订阅多个主题)
        consumer.subscribe(Arrays.asList("first", "second"), new ConsumerRebalanceListener() {
            // 该方法会在 Rebalance 之前调用
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                commitOffset(currentOffset);
            }

            // 该方法会在 Rebalance 之后调用
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                currentOffset.clear();
                for (TopicPartition partition : partitions) {
                    // 定位到最近提交的 offset 位置继续消费
                    consumer.seek(partition, getOffset(partition));
                }
            }
        });

        // Ctrl+C结束
        while (true) {
            // 4. 获取数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);

            // 5. 解析并打印consumerRecords
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println(record.key() + "--" + record.value());
            }

            // 异步提交
            commitOffset(currentOffset);

        }
    }

    // 获取某分区的最新 offset
    private static long getOffset(TopicPartition partition) {
        return 0;
    }

    // 提交该消费者所有分区的 offset
    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {

    }
}
