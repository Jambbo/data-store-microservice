package com.example.datastoremicroservice.repository;

import com.example.datastoremicroservice.model.MeasurementType;
import com.example.datastoremicroservice.model.Summary;
import com.example.datastoremicroservice.model.SummaryType;
import org.springframework.stereotype.Repository;
import com.example.datastoremicroservice.model.Data;

import java.util.Optional;
import java.util.Set;


public interface SummaryRepository{

    Optional<Summary> findBySensorId(
            long sensorId,
            Set<MeasurementType> measurementTypes,
            Set<SummaryType> summaryTypes);

    void handle(Data data);

}
