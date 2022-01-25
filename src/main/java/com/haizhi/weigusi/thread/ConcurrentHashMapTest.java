package com.haizhi.weigusi.thread;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 模拟信息的发送和接收
 */
public class ConcurrentHashMapTest {
    public static ConcurrentHashMap<Integer, String> pushMessage = new ConcurrentHashMap<Integer, String>();

    public static void main(String[] args) {
        for (int i = 0; i < 5; i++) {
            pushMessage.put(i, "该消息是id为" + i + "的消息");
        }

        Thread sendThread = new SendThread();
        Thread receiveThread = new ReceiveThread();
        sendThread.start();
        receiveThread.start();
        for (int i = 5; i < 10; i++) {
            pushMessage.put(i, "该消息是id为" + i + "的消息");
        }
    }
}
