package com.darren.service.config;

import com.darren.service.lock.DefaultWatcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZkUtil {

    private static String address = "192.168.244.21:2181,192.168.244.22:2181,192.168.244.23:2181,192.168.244.24:2181/mydata";
    private static DefaultWatcher watcher = new DefaultWatcher();
    private static ZooKeeper zk;
    static CountDownLatch cc = new CountDownLatch(1);

    public static ZooKeeper getZk(){
        try {
            zk = new ZooKeeper(address, 1000, watcher);
            watcher.setCc(cc);
            cc.await();
        } catch (IOException e) {
            e.printStackTrace();
        }catch (InterruptedException e) {
            e.printStackTrace();
        }
        return zk;
    }

}
