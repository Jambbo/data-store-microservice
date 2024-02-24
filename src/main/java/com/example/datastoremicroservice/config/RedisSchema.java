package com.example.datastoremicroservice.config;


        //Класс который описывает то как ключи будут называться

import com.example.datastoremicroservice.model.MeasurementType;

public class RedisSchema {

    //set
    // тут будут хранится все ключи сенсоров, айди всех сенсоров в сете редиса
    public static String sensorKeys(){
        return KeyHelper.getKey("sensors");
    }

    //hash with summary types
    public static String summaryKey(
            long sensorId,
            MeasurementType measurementType
    ){
        return KeyHelper.getKey("sensors:"+sensorId+":"+measurementType.name().toLowerCase());
    }
}
