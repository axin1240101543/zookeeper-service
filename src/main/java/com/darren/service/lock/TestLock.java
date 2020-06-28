package com.darren.service.lock;

import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
/**
 * 1、获取zk
 * 2、获取锁 -> 创建临时顺序节点
 * 3、回调 -> 获取子节点
 * 4、回调 -> 排序（减少成本、通信压力） -> 判断是否是第一个（序号最小的获取锁）
 *   是 -> 获取锁
 *   否 -> 判断前一个节点是否存在
 * 5、当前一个释放了锁（删除node），产生watcher
 *   下一个node重新获取数据，开始走步骤3
 */
public class TestLock {

    ZooKeeper zk;

    @Before
    public void before(){
        zk = ZkUtil.getZk();
    }

    @After
    public void after(){
        try {
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testLock(){

        for (int i = 0; i < 10; i++) {
            new Thread(){
                @Override
                public void run() {
                    ZkLock lock = new ZkLock();
                    lock.setZk(zk);
                    lock.setThreadName(Thread.currentThread().getName());
                    lock.tryLock();
                    System.out.println("to do work");
                    lock.unLock();
                }
            }.start();
        }

        while (true){

        }

    }
}
