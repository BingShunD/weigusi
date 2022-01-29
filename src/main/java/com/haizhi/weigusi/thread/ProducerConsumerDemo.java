package com.haizhi.weigusi.thread;


//资源类
class Resource{
    private String name;
    private int count=1;
    private boolean flag=false;
    public synchronized void set(String name){
        //生产资源
        while(flag) {
            try{
                //线程等待。消费者消费资源
                wait();
            }catch(Exception e){}
        }
        this.name=name+"---"+count++;
        System.out.println(Thread.currentThread().getName()+"...生产者..."+this.name);
        flag=true;
        //唤醒等待中的消费者
        this.notifyAll();
    }
    public synchronized void out(){
        //消费资源
        while(!flag) {
            //线程等待，生产者生产资源
            try{wait();}catch(Exception e){}
        }
        System.out.println(Thread.currentThread().getName()+"...消费者..."+this.name);
        flag=false;
        //唤醒生产者，生产资源
        this.notifyAll();
    }
}
//生产者
class Producer implements Runnable{
    private Resource res;
    Producer(Resource res){
        this.res=res;
    }
    //生产者生产资源
    public void run(){
        while(true){
            res.set("商品");
        }
    }
}
//消费者消费资源
class Consumer implements Runnable{
    private Resource res;
    Consumer(Resource res){
        this.res=res;
    }
    public void run(){
        while(true){
            res.out();
        }
    }
}
public class ProducerConsumerDemo{
    public static void main(String[] args){
        Resource r=new Resource();
        Producer pro=new Producer(r);
        Consumer con=new Consumer(r);
        Thread t1=new Thread(pro);
        Thread t2=new Thread(con);
        t1.start();
        t2.start();
    }
}

/*
    请说出同步线程及线程调度相关的方法？
        wait()：使一个线程处于等待（阻塞）状态，并且释放所持有的对象的锁；
        sleep()：使一个正在运行的线程处于睡眠状态，是一个静态方法，调用此方法要处理InterruptedException异常；
        notify()：唤醒一个处于等待状态的线程，当然在调用此方法的时候，并不能确切的唤醒某一个等待状态的线程，
        而是由JVM确定唤醒哪个线程，而且与优先级无关；
        notityAll()：唤醒所有处于等待状态的线程，该方法并不是将对象的锁给所有线程，而是让它们竞争，只有获得锁
        的线程才能进入就绪状态；
        注意：java 5 通过Lock接口提供了显示的锁机制，Lock接口中定义了加锁（lock（）方法）和解锁（unLock（）
        方法），增强了多线程编程的灵活性及对线程的协调
*/
