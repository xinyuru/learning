package com.ruxinyu.java.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Author: ruxinyu
 * @Description: 消费者消费数据
 * @Date: 14:12 2019/8/25
 **/
public class CustomerConsumer {
    public static void main(String[] args) {

        //配置信息
        Properties props = new Properties();
        //kafka集群
        props.put("bootstrap.servers", "hadoop-master:9092");
        //消费者组id
        props.put("group.id", "test");
        //设置自动提交offset
        props.put("enable.auto.commit", "true");
        //拿到数据后多长时间提交offset,可能会有重复数据
        props.put("auto.commit.interval.ms", "1000");
        //KV的反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //指定topic,可以消费多个分区
        consumer.subscribe(Arrays.asList("first","second","third"));

        while (true) {
            //获取数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);

            //处理数据
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println(record.topic() + "--" + record.partition() + "--" + record.value());
            }
        }
    }
}
