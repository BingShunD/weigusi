package com.haizhi.weigusi.thread;

public class Counter {
    private volatile int count = 0;
    public void inc(){
        try {
            Thread.sleep(3);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        count++;
    }
    @Override
    public String toString() {
        return "[count=" + count + "]";
    }
}
//---------------------------------华丽的分割线-----------------------------
class VolatileTest {
    public static void main(String[] args) {
        final Counter counter = new Counter();
        for(int i=0;i<1000;i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    counter.inc();
                }
            }).start();
        }
        System.out.println(counter);
    }
}



/*
        synchronized 和 volatile 关键字的作用
        一旦一个共享变量（类的成员变量、类的静态成员变量）被volatile修饰之后，那么就具备了两层语义：
        1）保证了不同线程对这个变量进行操作时的可见性，即一个线程修改了某个变量的值，这新值对其他线程来说是
        立即可见的。
        2）禁止进行指令重排序。
        volatile本质是在告诉jvm当前变量在寄存器（工作内存）中的值是不确定的，需要从主存中读取；
        synchronized则是锁定当前变量，只有当前线程可以访问该变量，其他线程被阻塞住。
        1.volatile仅能使用在变量级别；
        synchronized则可以使用在变量、方法、和类级别的
        2.volatile仅能实现变量的修改可见性，并不能保证原子性；
        synchronized则可以保证变量的修改可见性和原子性
        3.volatile不会造成线程的阻塞；
        synchronized可能会造成线程的阻塞。
        4.volatile标记的变量不会被编译器优化；
        synchronized标记的变量可以被编译器优化


        上面的代码执行完后输出的结果确定为1000吗？
        答案是不一定，或者不等于1000。这是为什么吗？
        在 java 的内存模型中每一个线程运行时都有一个线程栈，线程栈保存了线程运行时候变量值信息。
        当线程访问某一个对象时候值的时候，首先通过对象的引用找到对应在堆内存的变量的值，
        然后把堆内存变量的具体值load到线程本地内存中，建立一个变量副本，之后线程就不再和对象在堆内存变量值有任何关系，
        而是直接修改副本变量的值，在修改完之后的某一个时刻（线程退出之前），自动把线程变量副本的值回写到对象在堆中变量。
        这样在堆中的对象的值就产生变化了。
        也就是说上面主函数中开启了 1000 个子线程，每个线程都有一个变量副本，
        每个线程修改变量只是临时修改了自己的副本，当线程结束时再将修改的值写入在主内存中，这样就出现了线程安全问题。
        因此结果就不可能等于1000了，一般都会小于1000。
*/
