package com.darren.service.config;

import com.darren.service.lock.ZkUtil;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * 1、判断节点是否存在
 * 2、不存在
 *    不作任何操作，阻塞
 * 3、存在
 *    获取数据，返回
 * 4、节点创建或变更
 *    重新获取数据
 * 5、节点删除
 *    清空配置 + 重新阻塞
 */
public class TestConfig {

    ZooKeeper zk;

    CountDownLatch cc = new CountDownLatch(1);

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
    public void testConfig(){
        ZkConfig zkConfig = new ZkConfig();
        MyConf myConf = new MyConf();
        zkConfig.setZk(zk);
        zkConfig.setCc(cc);
        zkConfig.setMyConf(myConf);

        zkConfig.myWait();


        while (true){
            if (myConf.getConf().equals("")){
                System.out.println("no node ……");
                zkConfig.myWait();
            }else {
                System.out.println(myConf.getConf());
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
