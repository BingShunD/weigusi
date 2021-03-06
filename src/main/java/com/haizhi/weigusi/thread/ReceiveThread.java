package com.haizhi.weigusi.thread;

import java.util.Map.Entry;

/**
 * 接收发送过来的信息，并从内存中删除
 *
 */
public class ReceiveThread extends Thread {
    @Override
    public void run() {
        try {
            for (int i = 0; i < 100000; i++) {
                sleep(2000);
                for (Entry<Integer, String> map : ConcurrentHashMapTest.pushMessage.entrySet()) {
                    if (map.getKey() == i) {
                        System.out.println("成功收到id为：" + map.getKey() + "返回的信息，删除该元素");
                        ConcurrentHashMapTest.pushMessage.remove(map.getKey());
                    }
                }
                System.out.println("内存对象中的元素数量为：" + ConcurrentHashMapTest.pushMessage.size());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
