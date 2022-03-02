package com.gf.utils;

import java.sql.Timestamp;

public class ItemViewCountPerWindow {
    public String itemId;
    public Long count;
    public Long windowStartTime;
    public Long windowEndTime;

    public ItemViewCountPerWindow() {
    }

    public ItemViewCountPerWindow(String itemId, Long count, Long windowStartTime, Long windowEndTime) {
        this.itemId = itemId;
        this.count = count;
        this.windowStartTime = windowStartTime;
        this.windowEndTime = windowEndTime;
    }

    @Override
    public String toString() {
        return "ItemViewCountPerWindow{" +
                "itemId='" + itemId + '\'' +
                ", count=" + count +
                ", windowStartTime=" + new Timestamp(windowStartTime) +
                ", windowEndTime=" + new Timestamp(windowEndTime) +
                '}';
    }
}
