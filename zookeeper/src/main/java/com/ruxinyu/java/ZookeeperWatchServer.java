package com.ruxinyu.java;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @Author: ruxinyu
 * @Description: 服务器动态上下线
 * @Date: 21:04 2019/8/17
 **/
public class ZookeeperWatchServer {
    public static void main(String[] args) throws Exception{

        ZookeeperWatchServer zookeeperWatchServer=new ZookeeperWatchServer();
        //1 连接zookeeper服务器
        zookeeperWatchServer.getConnect();
        //2 注册节点
        zookeeperWatchServer.regist("hadoop");
        //3 业务逻辑
        zookeeperWatchServer.business();
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void regist(String hostname) throws KeeperException, InterruptedException {
        zkClient.create("/servers/server",hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname+" is online");
    }

    private String connectString="hadoop-master:2181,hadoop-slave01:2181,hadoop-slave02:2181";
    private int sessionTimeout=2000;
    private ZooKeeper zkClient;
    private void getConnect() throws IOException {
        zkClient=new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }
}
