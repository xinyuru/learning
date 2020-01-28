package com.ruxinyu.java;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: ruxinyu
 * @Description: 客户端监控服务器变化
 * @Date: 21:19 2019/8/17
 **/
public class ZookeeperClient {
    public static void main(String[] args) throws Exception{

        ZookeeperClient zookeeperClient=new ZookeeperClient();
        //1 获取zookeeper集群连接
        zookeeperClient.getConnect();
        //2 注册监听
        zookeeperClient.getChildren();
        //3 业务逻辑处理
        zookeeperClient.business();
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void getChildren() throws KeeperException, InterruptedException {

        List<String> children=zkClient.getChildren("/servers",true);
        //存储服务器主机名集合
        ArrayList<String> hosts=new ArrayList<>();
        for(String child:children){

            byte[] data=zkClient.getData("/servers/"+child,false,null);
            hosts.add(new String(data));
        }

        //将所有在线主机名称打印到控制台
        System.out.println(hosts);
    }

    private String connectString="hadoop-master:2181,hadoop-slave01:2181,hadoop-slave02:2181";
    private int sessionTimeout=2000;
    private ZooKeeper zkClient;
    private void getConnect() throws IOException {
        zkClient=new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                //多次执行getChildren
                try {
                    getChildren();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        });
    }
}
