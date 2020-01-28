package com.ruxinyu.java.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @Author: ruxinyu
 * @Description: 实现Partitioner,写入不同分区
 * @Date: 14:05 2019/8/25
 **/
public class CustomerPartitioner implements Partitioner {

    //可以hash之后插入不同分区
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return 1;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
