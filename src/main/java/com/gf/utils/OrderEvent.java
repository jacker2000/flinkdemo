package com.gf.utils;

public class OrderEvent {
    public String orderId;
    public String type;
    public Long ts;

    public OrderEvent() {
    }

    public OrderEvent(String orderId, String type, Long ts) {
        this.orderId = orderId;
        this.type = type;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId='" + orderId + '\'' +
                ", type='" + type + '\'' +
                ", ts=" + ts +
                '}';
    }
}
