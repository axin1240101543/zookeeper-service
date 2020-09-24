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
        //重入锁逻辑 begin
        /*String currentLockThreadName = null;
        try {
            byte[] data = zk.getData("/", false, new Stat());
            currentLockThreadName = new String(data);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (StringUtils.isNotBlank(currentLockThreadName) && threadName.equals(currentLockThreadName)){
            return;
        }*/
        //重入锁逻辑 end

		//每个线程创建序列临时节点
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
        //如果第一个线程释放了锁，只有第二个线程收到回调事件
        //如果其中某一个线程挂掉了，那么它后面的那个线程也能收到这个回调事件，从而去监控这个线程前面的那个线程
        switch (event.getType()) {
            case None:
                break;
            case NodeCreated:
                break;
            case NodeDeleted:
                //当上一个node被删除，重新获取list，下一个node重新去获取锁
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
	 * AsyncCallback.StringCallback
     * @param i
     * @param s
     * @param o
     * @param s1 节点的名字
     */
    @Override
    public void processResult(int i, String s, Object o, String s1) {
		//锁创建成功 s1不等于null
        if (s1 != null){
            System.out.println(threadName + " created node : " + s1);
            pathName = s1;
			// watcher：false -> 不需要监控父目录
            zk.getChildren("/", false, this, "abc");
        }
    }

    /**
     * getChildren回调
	 * AsyncCallback.Children2Callback
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
		//列表中最小的获取锁
        if (index == 0){
            //yes
            System.out.println(threadName + " get locked……");
            try {
				//目的1：为了避免释放锁太快，其他的线程还没有监控成功
				//目的2：可重入
				//将获得锁的线程名称写入根节点
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
	 * AsyncCallback.StatCallback
	 * zk.exists("/" + list.get(index-1), this, this, "abc"); -> 不知道是否监控成功
     * @param i
     * @param s
     * @param o
     * @param stat
     */
    @Override
    public void processResult(int i, String s, Object o, Stat stat) {
        //todo
        //如果当前列表的前一个节点不存在，那么重新获取list，重新去监控前一个节点
		if (null == stat){
            zk.getChildren("/", false, this, "abc");
        }
    }
}
