package com.ruxinyu.java.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @Author: ruxinyu
 * @Description: kafka Producer
 * @Date: 13:20 2019/8/25
 **/
public class CustomerProducer {
    public static void main(String[] args) {

        //配置信息
        Properties props=new Properties();
        //kafka集群
        props.put("bootstrap.servers","hadoop-master:9092");
        //ack应答级别,等价于props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put("acks","all");
        //重试次数
        props.put("retries",0);
        //批量大小
        props.put("batch.size",16384);
        //提交延时
        props.put("linger.ms",1);
        //缓存
        props.put("buffer.memory",33554432);
        //KV的序列化类
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        //设置放入哪个分区
        props.put("partitioner.class","com.ruxinyu.java.producer.CustomerPartitioner");

        //配置在ProducerConfig类中

        //创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        //循环发送数据,加入回调函数(写入成功后，调用该函数)
        for(int i=0;i<10;i++) {
            producer.send(new ProducerRecord<String, String>("first", String.valueOf(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception==null){
                        System.out.println(metadata.partition()+"--"+metadata.offset());
                    }
                    else {
                        System.out.println("发送失败");
                    }
                }
            });
        }
        //关闭资源
        producer.close();
    }
}
