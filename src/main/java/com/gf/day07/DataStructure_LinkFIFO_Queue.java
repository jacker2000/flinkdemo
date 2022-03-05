package com.gf.day07;

public class DataStructure_LinkFIFO_Queue {

    /**
     * 定义单向链表的节点
     */
    public static class Node {
        public Integer value;
        public Node next;

        public Node(Integer value) {
            this.value = value;
            this.next = null;
        }

        @Override
        public String toString() {
            return "Node{" +
                    "value=" + value +
                    ", next=" + next +
                    '}';
        }
    }

    public static class Queue {
        public Node head;
        public Integer count;//队列存放数据大小
        public Integer size;//队列总长度

        public Queue(Integer size) {
            this.head = new Node(0);
            this.count = 0;
            this.size = size;
        }

        @Override
        public String toString() {
            return "Queue{" +
                    "head=" + head +
                    ", count=" + count +
                    ", size=" + size +
                    '}';
        }

        public void push(Integer i) {
            if (count.equals(size)) {
                throw new RuntimeException("队列溢出，出现背压");
            } else {
                Node node = new Node(i);
                node.next = head.next; //把node的next节点指向head的next节点
                head.next = node;
                count++;
            }
        }

        public Integer pop() {
            if (count == 0) {
                throw new RuntimeException("出现饥饿状态，队列为空不能出队列");
            } else {
                Node tmp = head;
                while (tmp.next.next != null) {
                    tmp = tmp.next;
                }
                Integer result = tmp.next.value;
                tmp.next = null;
                count--;
                return result;
            }

        }
        public static void main(String[] args) {
            Queue queue = new Queue(3);
            queue.push(1);
            queue.push(2);
            queue.push(3);
            System.out.println(queue);
        }
    }
}
