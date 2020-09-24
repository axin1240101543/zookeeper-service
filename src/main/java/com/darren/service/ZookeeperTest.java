package com.darren.service;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Hello world!
 *
 * Zookeeper API基本使用
 * Tips：如果使用虚拟机，记得配置windows的hosts文件
 */
public class ZookeeperTest {
    public static void main( String[] args ) throws IOException, KeeperException, InterruptedException {
        System.out.println( "Hello World!" );

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        /**
         * zk是有session概念的，没有连接池的概念
         * watch：观察 、 回调
         * watch的注册只发生在 读 类型调用，get、exists
         * 第一类：new zk 时候，传入的watch，这个watch，session级别的，跟path 、node没有关系。
         * 只能收到关于session的连接或者某个server断开重新连接别的server
         * 第二类：node的增删改
         *
         * connectString：连接的zk集群ips
         * sessionTimeout：session超时时间，单位：毫秒
         * watcher：事件回调
         */
        final ZooKeeper zk = new ZooKeeper("192.168.11.11:2181, 192.168.11.12:2181, 192.168.11.13:2181, 192.168.11.14:2181",
                3000, new Watcher() {
            //Watcher的回调方法
            @Override
            public void process(WatchedEvent event) {
                System.out.println("new zk event:" + event.toString());
                Event.KeeperState state = event.getState();
                Event.EventType type = event.getType();
                //zk event的状态
                switch (state) {
                    case Unknown:
                        break;
                    case Disconnected:
                        break;
                    case NoSyncConnected:
                        break;
                    case SyncConnected:
                        //建立zk连接
                        countDownLatch.countDown();
                        break;
                    case AuthFailed:
                        break;
                    case ConnectedReadOnly:
                        break;
                    case SaslAuthenticated:
                        break;
                    case Expired:
                        break;
                    case Closed:
                        break;
                }
                //zk event的类型
                switch (type) {
                    case None:
                        break;
                    case NodeCreated:
                        break;
                    case NodeDeleted:
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
            }});
        //等待zk连接成功
        countDownLatch.await();
        //zk的状态
        ZooKeeper.States state = zk.getState();
        switch (state) {
            case CONNECTING:
                //正在连接zk
                System.out.println("zk status connecting ……");
                break;
            case ASSOCIATING:
                break;
            case CONNECTED:
                //已经连接zk
                System.out.println("zk status connected ……");
                break;
            case CONNECTEDREADONLY:
                break;
            case CLOSED:
                break;
            case AUTH_FAILED:
                break;
            case NOT_CONNECTED:
                break;
        }


        /**
         * path：节点
         * data：数据 -> 二进制安全的 -> 字节数组
         * acl: 访问权限 -> OPEN_ACL_UNSAFE -> 无权限
         * createMode：创建什么节点：永久、临时、顺序永久、顺序临时
         * StringCallback：回调
         * ctx：context
         */
        String nodeName = zk.create("/Darren", "old data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("nodeName:" + nodeName);

        final Stat stat = new Stat();

        /**
         * path：节点
         * watcher：事件回调
         * stat：元数据
         * DataCallBack：回调
         * ctx：context
         *
         * watcher可以new一个新的watcher
         * watcher为boolean，true -> 注册到new zk的那个watcher
         * watcher为this -> 注册到当前的watcher
         */
        byte[] data = zk.getData("/Darren", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("get data event:" + event.toString());
                try {
                    //在当前的watcher中重新注册
                    zk.getData("/Darren", this, stat);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, stat);
        System.out.println("get data:" + new String(data));

        /**
         * path：节点
         * data：数据
         * version：版本
         *
         * 这个更新操作会触发上面的get data注册的事件
         */
        Stat stat1 = zk.setData("/Darren", "new data".getBytes(), 0);

        /**
         * 这个更新操作不会触发上面的get data注册的事件，因为事件是一次性回调的
         *
         * 那那那如果想要再次触发，应该怎么做？
         * 在上面的get data中重新注册
         */
        zk.setData("/Darren", "new data 007".getBytes(), stat1.getVersion());


        //-------------------------异步回调-----------------
        /**
         * status：状态
         * path：路径
         * ctx：上下文
         * data：数据
         * stat：元数据
         */
        System.out.println("async start");
        zk.getData("/Darren", false, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int status, String path, Object ctx, byte[] data, Stat stat) {
                System.out.println("async call back ……");
                System.out.println(new String(data));
                //就是传入的ctx
                System.out.println(ctx.toString());
            }
        }, "context");
        System.out.println("async end");

        while (true){

        }
    }

}