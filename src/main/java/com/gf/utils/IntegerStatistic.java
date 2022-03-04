package com.gf.utils;

public  class IntegerStatistic{
    public Integer max;
    public Integer min;
    public Integer sum;
    public Integer count;
    public Integer avg;

    public IntegerStatistic() {
    }

    public IntegerStatistic(Integer max, Integer min, Integer sum, Integer count, Integer avg) {
        this.max = max;
        this.min = min;
        this.sum = sum;
        this.count = count;
        this.avg = avg;
    }

    @Override
    public String toString() {
        return "IntegerStatistic{" +
                "max=" + max +
                ", min=" + min +
                ", sum=" + sum +
                ", count=" + count +
                ", avg=" + avg +
                '}';
    }
}
