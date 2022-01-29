package com.haizhi.weigusi.thread;

import java.util.concurrent.Semaphore;

public class ThreadCommunication {
    private static int num;
    /**
     * 定义一个信号量，该类内部维持了多个线程锁，可以阻塞多个线程，释放多个线程，
     线程的阻塞和释放是通过 permit 概念来实现的
     * 线程通过 semaphore.acquire()方法获取 permit，如果当前 semaphore 有 permit 则分配给该线程，
     如果没有则阻塞该线程直到 semaphore
     * 调用 release（）方法释放 permit。
     * 构造函数中参数：permit（允许） 个数，
     */
    private static Semaphore semaphore = new Semaphore(0);
    public static void main(String[] args) {

        Thread threadA = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    //模拟耗时操作之后初始化变量 num
                    Thread.sleep(1000);
                    num = 1;
                    //初始化完参数后释放两个 permit
                    semaphore.release(2);

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        Thread threadB = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    //获取 permit，如果 semaphore 没有可用的 permit 则等待，如果有则消耗一个
                    semaphore.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(Thread.currentThread().getName()+"获取到 num 的值为："+num);
            }
        });
        Thread threadC = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    //获取 permit，如果 semaphore 没有可用的 permit 则等待，如果有则消耗一个
                    semaphore.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(Thread.currentThread().getName()+"获取到 num 的值为："+num);
            }
        });
        //同时开启 3 个线程
        threadA.start();
        threadB.start();
        threadC.start();

    }
}

/*

         三个线程 a、b、c 并发运行，b,c 需要 a 线程的数据怎么实现？
             根据问题的描述，我将问题用以下代码演示，ThreadA、ThreadB、ThreadC，ThreadA 用于初始化数据 num，只有当num初始化完成之后再让ThreadB和ThreadC获取到初始化后的变量num。
             分析过程如下：
             考虑到多线程的不确定性，因此我们不能确保ThreadA就一定先于ThreadB和ThreadC前执行，就算ThreadA先执行了，我们也无法保证ThreadA什么时候才能将变量num给初始化完成。因此我们必须让ThreadB和ThreadC去等待ThreadA完成任何后发出的消息。
             现在需要解决两个难题，一是让 ThreadB 和 ThreadC 等待 ThreadA 先执行完，二是 ThreadA 执行完之后给ThreadB和ThreadC发送消息。
             解决上面的难题我能想到的两种方案，一是使用纯Java API的Semaphore类来控制线程的等待和释放，二是使用Android提供的Handler消息机制。


        同一个类中的 2 个方法都加了同步锁，多个线程能同时访问同一个类中的这两个方法吗？
            这个问题需要考虑到Lock与synchronized 两种实现锁的不同情形。因为这种情况下使用Lock 和synchronized 会有截然不同的结果。
            Lock可以让等待锁的线程响应中断，Lock获取锁，之后需要释放锁。如下代码，多个线程不可访问同一个类中的2个加了Lock锁的方法。
            而synchronized却不行，使用synchronized时，当我们访问同一个类对象的时候，是同一把锁，所以可以访问该对象的其他synchronized方法。


*/
