package com.darren.service.config;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

public class ZkConfig implements Watcher, AsyncCallback.StatCallback, AsyncCallback.DataCallback {

    ZooKeeper zk;

    CountDownLatch cc;

    MyConf myConf;

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public void setCc(CountDownLatch cc) {
        this.cc = cc;
    }

    public void setMyConf(MyConf myConf) {
        this.myConf = myConf;
    }

    /**
     * Watcher
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                break;
            case NodeCreated:
                System.out.println("node created");
                zk.getData("/conf", this, this, "abc");
                break;
            case NodeDeleted:
                myConf.setConf("");
                cc = new CountDownLatch(1);
                break;
            case NodeDataChanged:
                System.out.println("node updated");
                zk.getData("/conf", this, this, "abc");
                break;
            case NodeChildrenChanged:
                break;
            case DataWatchRemoved:
                break;
            case ChildWatchRemoved:
                break;
            case PersistentWatchRemoved:
                break;
        }
    }

    public void myWait(){
        zk.exists("/conf", this, this, "abc");
        try {
            cc.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * exists
     * @param i
     * @param s
     * @param o
     * @param stat
     */
    @Override
    public void processResult(int i, String s, Object o, Stat stat) {
        if (stat != null){
            zk.getData("/conf", this, this, "abc");
        }
    }

    /**
     * getData
     * @param i
     * @param s
     * @param o
     * @param bytes
     * @param stat
     */
    @Override
    public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
        if (stat != null){
            myConf.setConf(new String(bytes));
            cc.countDown();
        }
    }
}
