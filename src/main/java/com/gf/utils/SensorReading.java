package com.gf.utils;

public class SensorReading {
    public String sensorId;
    public Double temperature;

    public SensorReading(String sensorId, Double temperature) {
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

