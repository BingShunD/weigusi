package com.haizhi.weigusi.thread;

public class ThreadLocalTest {
    //ThreadLocal是除了加锁这种同步方式之外的一种保证一种规避多线程访问出现线程不安全的方法，
    // 当我们在创建一个变量后，如果每个线程对其进行访问的时候访问的都是线程自己的变量这样就不会存在线程不安全问题。
    static ThreadLocal<String> localVar = new ThreadLocal<>();

    static void print(String str) {
        //打印当前线程中本地内存中本地变量的值
        System.out.println(str + " :" + localVar.get());
        //清除本地内存中的本地变量
        localVar.remove();
    }

    public static void main(String[] args) {
        Thread t1 = new Thread(new Runnable() {
            public void run() {
                //设置线程1中本地变量的值
                localVar.set("localVar1");
                //调用打印方法
                print("thread1");
                //打印本地变量
                System.out.println("after remove : " + localVar.get());
            }
        });

        Thread t2 = new Thread(new Runnable() {
            public void run() {
                //设置线程1中本地变量的值
                localVar.set("localVar2");
                //调用打印方法
                print("thread2");
                //打印本地变量
                System.out.println("after remove : " + localVar.get());
            }
        });

        t1.start();
        t2.start();
    }
}


/*
        newSingleThreadExecutor：创建一个单线程的线程池，此线程池保证所有任务的执行顺序按照任务的提交顺序执行。
        newFixedThreadPool：创建固定大小的线程池，每次提交一个任务就创建一个线程，直到线程达到线程池的最大大小。
        newCachedThreadPool：创建一个可缓存的线程池，此线程池不会对线程池大小做限制，线程池大小完全依赖于操作系统（或者说JVM）能够创建的最大线程大小。
        newScheduledThreadPool：创建一个大小无限的线程池，此线程池支持定时以及周期性执行任务的需求。

        1. ExecutorService newCachedThreadPool = Executors.newCachedThreadPool();
        2. ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(4);
        3. ScheduledExecutorService newScheduledThreadPool = Executors.newScheduledThreadPool(4);
        4. ExecutorService newSingleThreadExecutor = Executors.newSingleThreadExecutor();

        请叙述一下您对线程池的理解？
        第一：降低资源消耗。通过重复利用已创建的线程降低线程创建和销毁造成的消耗。
        第二：提高响应速度。当任务到达时，任务可以不需要等到线程创建就能立即执行。
        第三：提高线程的可管理性。线程是稀缺资源，如果无限制的创建，不仅会消耗系统资源，还会降低系统的稳定性，
        使用线程池可以进行统一的分配，调优和监控。

         线程池的启动策略？
         1、线程池刚创建时，里面没有一个线程。任务队列是作为参数传进来的。不过，就算队列里面有任务，线程池也不会马上执行它们。
         2、当调用execute() 方法添加一个任务时，线程池会做如下判断：
         a. 如果正在运行的线程数量小于 corePoolSize，那么马上创建线程运行这个任务；
         b. 如果正在运行的线程数量大于或等于 corePoolSize，那么将这个任务放入队列。
         c. 如果这时候队列满了，而且正在运行的线程数量小于 maximumPoolSize，那么还是要创建线程运行这个任务；
         d. 如果队列满了，而且正在运行的线程数量大于或等于 maximumPoolSize，那么线程池会抛出异常，告诉调用者“我不能再接受任务了”。
         3、当一个线程完成任务时，它会从队列中取下一个任务来执行。
         4、当一个线程无事可做，超过一定的时间（keepAliveTime）时，线程池会判断，如果当前运行的线程数大于corePoolSize，那么这个线程就被停掉。所以线程池的所有任务完成后，它最终会收缩到 corePoolSize 的大小。
*/
