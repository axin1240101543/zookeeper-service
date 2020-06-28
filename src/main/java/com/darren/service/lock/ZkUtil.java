package com.darren.service.lock;

import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 连接zk，并返回zk
 */
public class ZkUtil {

    private static String address = "192.168.244.21:2181,192.168.244.22:2181,192.168.244.23:2181,192.168.244.24:2181/mydata";
    private static DefaultWatcher watcher = new DefaultWatcher();
    private static CountDownLatch cc = new CountDownLatch(1);
    private static ZooKeeper zk;


    public static ZooKeeper getZk(){
        try {
            zk = new ZooKeeper(address, 3000, watcher);
            watcher.setCc(cc);
            //阻塞，只有当连接完成，才返回
            cc.await();
        } catch (IOException e) {
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }
        return zk;
    }

}
