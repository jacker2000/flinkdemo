package com.gf.day07;

import java.util.ArrayList;

public class DataStructure_ArrFIFO_Queue {
    public static void main(String[] args) {
        Queue queue = new Queue(3);
        queue.push(1);
        queue.push(2);
        queue.push(3);
        System.out.println(queue);
//        for (int i = 0; i < queue.size; i++) {System.out.println(queue[i]);}
//        queue.push(4);
    }
    public static class Queue{
        private ArrayList<Integer> queue;
        private Integer size;

        //构造
        public Queue(Integer size) {
            this.size = size;
            queue = new ArrayList<>();
        }

        @Override
        public String toString() {
            return "Queue{" +
                    "queue=" + queue  +
                    '}';
        }

        //入队列
        public void push(Integer i){
            //判断当前数组是否满了
            if (queue.size()==size) {
                throw new RuntimeException("队列溢出，出现背压");
            }else {
                //添加元素
                queue.add(i);
            }
        }
        //出队列
        public Integer pop(){
            //判断当前数组是否已经不再有数据
            if (queue.size()==0) {
                throw new RuntimeException("出现饥饿状态，队列为空不能出队列");
            }else {
                //获取第一个，然后移除
                Integer result = queue.get(0);
                queue.remove(0);
                return result;
            }
        }
    }
}
