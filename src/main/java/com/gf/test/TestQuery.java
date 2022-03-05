package com.gf.test;

/**
 * todo
 *  数组和链表分别实现队列
 */
public class TestQuery<T> {
    private int head; //队头，
    private int tail; //队尾
    private int count; //元素个数
    private T[] queue;// 队列数组
    //无参构造
    public TestQuery() {
        //初始化数组
        queue = (T[])new Object[10];
        this.head = 0;//头下标为0
        this.tail = 0;
        this.count = 0;
    }
    //定义指定大小的队列
    public TestQuery(int size){
        queue =  (T[])new Object[size];
        this.head = 0;//头下标为0
        this.tail = 0;
        this.count = 0;
    }
    //入队
    public boolean inQueue(T t){
        if (queue.length==count) {
            return false;
        }
        queue[tail++%(queue.length)]= t;//不为空就放下一个
        count++;
        return true;
    }
    //出队
    public T outQueue(){
        if (count==0) {
            return null;
        }
        count--;
        return queue[head++%queue.length];
    }
    //查队头
    public T showHead(){
        if (count==0) {
            return null;
        }
        return queue[head];
    }
    //判满
    public boolean isFull(){
        return count==queue.length;
    }
    //判空
    public boolean isEmpty(){
        return count==0;
    }

    //测试
    public static void main(String[] args) {
        TestQuery<Integer> query = new TestQuery<>(10);
//        query.inQueue(1);
//        query.inQueue(2);
        for (int i = 1; i <= 10; i++) {
            query.inQueue(i);
        }
//        query.outQueue();
        System.out.println("队空?"+query.isEmpty());

        System.out.println("队满?"+query.isFull());
        System.out.println("对元素?"+query.showHead());
    }
}
