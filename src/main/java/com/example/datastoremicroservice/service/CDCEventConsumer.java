package com.example.datastoremicroservice.service;
    //CDC - Changes Data Capture, изменение данных и то как мы это сохраняем
// тут будут читаться из кафки из очереди те изменения, события которые произошли с базой, будем слушать базу с данными сенсоров
public interface CDCEventConsumer {

    void handle(String message); // т.к. в кафке храним меседжи

}
