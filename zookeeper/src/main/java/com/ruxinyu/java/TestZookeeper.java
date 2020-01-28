package com.ruxinyu.java;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @Author: ruxinyu
 * @Description: zookeeper连接测试
 * @Date: 2019/8/17 10:27
 **/


public class TestZookeeper {
        private String connectString="hadoop-master:2181,hadoop-slave01:2181,hadoop-slave02:2181";
        private int sessionTimeout=4000;
        private ZooKeeper zkClient;

        @Before
        public void init() throws IOException {

                zkClient=new ZooKeeper(connectString, sessionTimeout, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                        //获取子节点 并监控节点的变化
                        System.out.println("-----------start----------------");
                        List<String> children= null;
                        try {
                                children = zkClient.getChildren("/",true);
                        } catch (KeeperException e) {
                                e.printStackTrace();
                        } catch (InterruptedException e) {
                                e.printStackTrace();
                        }

                        for(String child:children){
                                System.out.println(child);
                        }
                        System.out.println("---------------end-----------------");

                }
            });
        }

        //创建节点
        @Test
        public void createNode() throws KeeperException,InterruptedException{
                String path = zkClient.create("/ruxinyu", "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                System.out.println(path);
        }

        //获取子节点 并监控节点的变化
        @Test
        public void getDataAndWatch() throws KeeperException, InterruptedException {
                Thread.sleep(Long.MAX_VALUE);
        }

        //判断节点是否存在
        @Test
        public void exist() throws KeeperException, InterruptedException {
                Stat stat=zkClient.exists("/ruxinyu",false);
                System.out.println(stat==null?"not exists":"exists");
        }



}