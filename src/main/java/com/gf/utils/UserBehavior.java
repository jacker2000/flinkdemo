package com.gf.utils;

public class UserBehavior {
    public String userId;
    public String itemId;
    public String categoryId;
    public String type;
    public Long ts;

    public UserBehavior() {
    }

    public UserBehavior(String userId, String itemId, String categoryId, String type, Long ts) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.type = type;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId='" + userId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", categoryId='" + categoryId + '\'' +
                ", type='" + type + '\'' +
                ", ts=" + ts +
                '}';
    }
}
