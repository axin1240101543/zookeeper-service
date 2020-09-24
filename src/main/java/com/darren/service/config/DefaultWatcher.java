package com.darren.service.config;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.concurrent.CountDownLatch;

/**
 * 默认的watcher
 */
public class DefaultWatcher implements Watcher {

    CountDownLatch cc;

    public void setCc(CountDownLatch cc) {
        this.cc = cc;
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("new zk event : " + event.toString());
        switch (event.getState()) {
            case Unknown:
                break;
            case Disconnected:
                break;
            case NoSyncConnected:
                break;
            case SyncConnected:
                System.out.println("SyncConnected");
                cc.countDown();
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
    }
}
