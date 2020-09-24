package com.darren.service.config;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

/**
 * 获取zk配置类
 */
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
				//节点被创建事件，重新获取数据
                System.out.println("node created");
                zk.getData("/conf", this, this, "abc");
                break;
            case NodeDeleted:
				//节点被删除事件，清空配置并重新阻塞
                myConf.setConf("");
                cc = new CountDownLatch(1);
                break;
            case NodeDataChanged:
				//节点内容被更新事件，重新获取数据
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
			//阻塞等待数据获取完成
            cc.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * exists
	 * AsyncCallback.StatCallback
     * @param i
     * @param s
     * @param o
     * @param stat
     */
    @Override
    public void processResult(int i, String s, Object o, Stat stat) {
        if (stat != null){
			//当节点存在，则去获取数据
            zk.getData("/conf", this, this, "abc");
        }
    }

    /**
     * getData
	 * AsyncCallback.DataCallback
     * @param i
     * @param s
     * @param o
     * @param bytes
     * @param stat
     */
    @Override
    public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
        if (stat != null){
			//获取到数据，设置到配置类中并CountDownLatch-1
            myConf.setConf(new String(bytes));
            cc.countDown();
        }
    }
}
