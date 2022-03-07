package com.gf.test;

public class TestQ {
    private int size;

    public TestQ(int size) {
        this.size = size;

    }

    public static void main(String[] args) {
        TestQ testQ = new TestQ(2);
        System.out.println(testQ.size);
    }
}
