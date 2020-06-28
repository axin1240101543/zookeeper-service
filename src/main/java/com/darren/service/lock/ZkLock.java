package com.darren.service.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZkLock implements Watcher, AsyncCallback.StringCallback, AsyncCallback.Children2Callback, AsyncCallback.StatCallback {

    ZooKeeper zk;

    String threadName;

    String pathName;

    CountDownLatch cc = new CountDownLatch(1);

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    /**
     * 获取锁
     */
    public void tryLock(){
        System.out.println(threadName + " create ……");
        zk.create("/lock", threadName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL,this, "abc");
        try {
            cc.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 释放锁
     */
    public void unLock(){
        try {
            zk.delete(pathName, -1);
            System.out.println(threadName + " ending……");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    /**
     * WatchedEvent回调
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                break;
            case NodeCreated:
                break;
            case NodeDeleted:
                //当上一个node被删除，下一个node重新去获取锁
                zk.getChildren("/", false, this, "abc");
                break;
            case NodeDataChanged:
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

    /**
     * create回调
     * @param i
     * @param s
     * @param o
     * @param s1 节点的名字
     */
    @Override
    public void processResult(int i, String s, Object o, String s1) {
        if (s1 != null){
            System.out.println(threadName + " created node : " + s1);
            pathName = s1;
            zk.getChildren("/", false, this, "abc");
        }
    }

    /**
     * getChildren回调
     * @param i
     * @param s
     * @param o
     * @param list
     * @param stat
     */
    @Override
    public void processResult(int i, String s, Object o, List<String> list, Stat stat) {
        //创建的顺序是一致，返回是乱序的
        Collections.sort(list);
        int index = list.indexOf(pathName.substring(1));
        if (index == 0){
            //yes
            System.out.println(threadName + " get locked……");
            try {
                zk.setData("/", threadName.getBytes(), -1);
                cc.countDown();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }else{
            //no
            //去watch前面一个Ephemeral node是否存在，而不是watch整个node
            //通过这个watcher来重新判断获取锁
            zk.exists("/" + list.get(index-1), this, this, "abc");
        }
    }

    /**
     * exists回调
     * @param i
     * @param s
     * @param o
     * @param stat
     */
    @Override
    public void processResult(int i, String s, Object o, Stat stat) {
        //todo
    }
}
