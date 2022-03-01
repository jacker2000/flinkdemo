package com.gf.utils;

public class SensorReading {
    public String sensorId;
    public Integer temperature;

    public SensorReading(String sensorId, Integer temperature) {
        this.sensorId = sensorId;
        this.temperature = temperature;
    }

    public SensorReading() {
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "sensorId='" + sensorId + '\'' +
                ", temperature=" + temperature +
                '}';
    }
}

