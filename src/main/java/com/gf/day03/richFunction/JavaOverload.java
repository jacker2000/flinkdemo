package com.gf.day03.richFunction;

public class JavaOverload {
    public static void main(String[] args) {
        //运算符重载
        add(1,2);
        add("hello","world");
        add(1,"a");
    }
    public static void add(Object a,Object b){
        if (a instanceof Integer && b instanceof Integer) {
            System.out.println((Integer)a +(Integer)b);
        } else if(a instanceof String && b instanceof String){
            System.out.println( a.toString() + b.toString());
        }else {
            System.out.println("类型错误!!");
        }
    }
}
