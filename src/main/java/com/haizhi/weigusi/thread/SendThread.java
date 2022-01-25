package com.haizhi.weigusi.thread;

import java.util.Map.Entry;

public class SendThread extends Thread {
    @Override
    public void run() {
        try {
            sleep(6000);
            while (ConcurrentHashMapTest.pushMessage.size() > 0) {
                for (Entry<Integer, String> hashMap : ConcurrentHashMapTest.pushMessage.entrySet()) {
                    System.out.println("消息id:" + hashMap.getKey()+ "未发送成功，在此重发:" + hashMap.getValue());
                }

                sleep(1000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
