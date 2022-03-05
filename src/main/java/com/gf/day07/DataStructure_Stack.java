package com.gf.day07;

import java.util.ArrayList;

/**
 *  数组方式实现栈
 */
public class DataStructure_Stack {

    public static class Stack{
        private ArrayList<Integer> stack;
        private Integer size;

        public Stack(Integer size) {
            this.stack = new ArrayList<>();
            this.size = size;
        }
        public void push(Integer i){
            if (stack.size()==size) {
                throw new RuntimeException("栈溢出");
            }else {
                stack.add(i);
            }
        }
        public Integer pop(){
            if (stack.size()==0) {
                throw new RuntimeException("栈为空，不能弹栈");
            }else {
                Integer result = stack.get(stack.size() - 1);
                stack.remove(stack.size()-1);
                return result;
            }
        }
    }
    public static void main(String[] args) {
        Stack stack = new Stack(3);
        stack.push(1);
        stack.push(2);
        stack.push(3);
        System.out.println(stack.pop());
        System.out.println(stack.pop());
        System.out.println(stack.pop());
    }

}
