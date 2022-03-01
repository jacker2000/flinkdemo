package com.gf.test;

import org.apache.flink.util.MathUtils;

public class Test {
    public static void main(String[] args) {
        Integer  i =0;
        Integer j =1;
        Integer z= 2;
//        System.out.println(i.hashCode());
//        System.out.println(j.hashCode());
//        System.out.println(z.hashCode());

        /**
         * 593689054
         * 68075478
         * 1085422463
         */
        System.out.println(MathUtils.murmurHash(i)+":"+MathUtils.murmurHash(i)%4);
        System.out.println(MathUtils.murmurHash(j)+":"+MathUtils.murmurHash(j)%4);
        System.out.println(MathUtils.murmurHash(z)+":"+MathUtils.murmurHash(z)%4);

    }
}
