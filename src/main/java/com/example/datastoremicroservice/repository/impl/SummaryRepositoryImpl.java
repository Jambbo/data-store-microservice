package com.example.datastoremicroservice.repository.impl;

import com.example.datastoremicroservice.config.RedisSchema;
import com.example.datastoremicroservice.model.Data;
import com.example.datastoremicroservice.model.MeasurementType;
import com.example.datastoremicroservice.model.Summary;
import com.example.datastoremicroservice.model.SummaryType;
import com.example.datastoremicroservice.repository.SummaryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Optional;
import java.util.Set;

@RequiredArgsConstructor
@Repository
public class SummaryRepositoryImpl implements SummaryRepository {

    private final JedisPool jedisPool; // будем обращаться к пулу редиса, чтобы работать не в
    // 1 потоке, а мы будем иметь пул из 8 или 16 потоков и доставать их по необходимости


    //В этом методе идет проверка на то какие аргументы передали в метод findBySensorId()
// если не передали measurementTypes(он пустой) то возвращаются все и тоже самое с summaryTypes
    @Override
    public Optional<Summary> findBySensorId(
            long sensorId,
            Set<MeasurementType> measurementTypes,
            Set<SummaryType> summaryTypes
    ) {
        try (Jedis jedis = jedisPool.getResource()) {
            if (!jedis.sismember(
                    RedisSchema.sensorKeys(),
                    String.valueOf(sensorId)
            )) {
                return Optional.empty();
            }
            if (measurementTypes.isEmpty() && !summaryTypes.isEmpty()) {
                return getSummary(
                        sensorId,
                        Set.of(MeasurementType.values()),
                        summaryTypes,
                        jedis
                );
            } else if (!measurementTypes.isEmpty() && summaryTypes.isEmpty()) {
                return getSummary(
                        sensorId,
                        measurementTypes,
                        Set.of(SummaryType.values()),
                        jedis
                );
            } else {
                return getSummary(
                        sensorId,
                        measurementTypes,
                        summaryTypes,
                        jedis
                );
            }
        }
    }


    private Optional<Summary> getSummary(
            long sensorId,
            Set<MeasurementType> measurementTypes,
            Set<SummaryType> summaryTypes,
            Jedis jedis
    ) {
        Summary summary = new Summary();
        summary.setSensorId(sensorId);
        for (MeasurementType mt : measurementTypes) {
            for (SummaryType st : summaryTypes) {
                Summary.SummaryEntry entry = new Summary.SummaryEntry();
                entry.setType(st);
                // получаем значение которое мы храним в базе с помощью обращения к хешу в редисе
                String value = jedis.hget(
                        RedisSchema.summaryKey(sensorId, mt),
                        st.name().toLowerCase() // поле к которому нам нужно обратиться
                );
                if (value != null) {
                    entry.setValue(Double.parseDouble(value)); // т.к. редис работает со строками и возвращает данные в строках
                }
                String counter = jedis.hget(
                        RedisSchema.summaryKey(sensorId, mt),
                        "counter"   // обращение к этому полю
                );
                if (counter != null) {
                    entry.setCounter(Long.parseLong(counter));
                }
                summary.addValue(mt, entry);
            }
        }
        return Optional.of(summary);
    }

    @Override
    public void handle(Data data) {
        try (Jedis jedis = jedisPool.getResource()) {
            if (!jedis.sismember(
                    RedisSchema.sensorKeys(),
                    String.valueOf(data.getSensorId()) //проверка, если у нас такой датчик уже добавлен, то тогда ничего не делаем и проходим дальше
            )) {
                jedis.sadd( // если не добавлен то добавляем его в sadd с айди всех датчиков, так мы можем знать какие датчики у нас есть
                        RedisSchema.sensorKeys(),
                        String.valueOf(data.getSensorId())
                );
            }
            updateMinValue(data, jedis);
            updateMaxValue(data, jedis);
            updateSumAndAvgValue(data, jedis);

        }
    }

    private void updateMinValue(Data data, Jedis jedis) {
// берем ключ, чтобы его использовать в нескольких местах
        String key = RedisSchema.summaryKey(
                data.getSensorId(),
                data.getMeasurementType()
        );
// получаем минимальное значение для данного типа изменения(на 2 строки выше) и для данного датчика, которое нам пришло
        String value = jedis.hget(
                key,
                SummaryType.MIN.name().toLowerCase()
        );
// проверяем есть ли это значение вообще
        if (value == null || data.getMeasurement() < Double.parseDouble(value)) {
            jedis.hset(
                    key,
                    SummaryType.MIN.name().toLowerCase(),
                    String.valueOf(data.getMeasurement())
            );
        }
    }

    private void updateMaxValue(Data data, Jedis jedis) {
// берем ключ, чтобы его использовать в нескольких местах
        String key = RedisSchema.summaryKey(
                data.getSensorId(),
                data.getMeasurementType()
        );
// получаем максимальное значение для данного типа изменения(на 2 строки выше) и для данного датчика, которое нам пришло
        String value = jedis.hget(
                key,
                SummaryType.MAX.name().toLowerCase()
        );
        if (value == null || data.getMeasurement() > Double.parseDouble(value)) {
            jedis.hset(
                    key,
                    SummaryType.MAX.name().toLowerCase(),
                    String.valueOf(data.getMeasurement())
            );
        }
    }

    private void updateSumAndAvgValue(Data data, Jedis jedis) {
        updateSumValue(data, jedis);
        String key = RedisSchema.summaryKey(
                data.getSensorId(),
                data.getMeasurementType()
        );
        String counter = jedis.hget(
                key,
                "counter"
        );
        if (counter == null) {
            counter = String.valueOf(
                    jedis.hset( //и hset'a возвращается новое значение, которое установилось, по этому и присваиваем counter
                            key,
                            "counter", // название поля, которое будет хранится
                            String.valueOf(1)
                    )
            );
        } else {
            counter = String.valueOf(
                    jedis.hincrBy(
                            key,
                            "counter",
                            1   //то есть инкрементируется на еденицу counter по ключу key
                    )
            );
        }
        String sum = jedis.hget(key, SummaryType.SUM.name().toLowerCase());
        jedis.hset(
                key,
                SummaryType.AVG.name().toLowerCase(),
                String.valueOf(
                        Double.parseDouble(sum) / Double.parseDouble(counter)
                )
        );
    }

    private void updateSumValue(Data data, Jedis jedis) {
        String key = RedisSchema.summaryKey(
                data.getSensorId(),
                data.getMeasurementType()
        );
        String value = jedis.hget(
                key,
                SummaryType.SUM.name().toLowerCase()
        );
        if (value == null) {
            jedis.hset(
                    key,
                    SummaryType.SUM.name().toLowerCase(),
                    String.valueOf(data.getMeasurement())
            );
        } else {
            jedis.hincrByFloat(
                    key,
                    SummaryType.SUM.name().toLowerCase(),
                    data.getMeasurement() // инкрементируем на то что нам пришло
            );
        }
    }


}
