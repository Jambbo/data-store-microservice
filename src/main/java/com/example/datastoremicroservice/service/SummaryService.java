package com.example.datastoremicroservice.service;

import com.example.datastoremicroservice.model.Data;
import com.example.datastoremicroservice.model.MeasurementType;
import com.example.datastoremicroservice.model.Summary;
import com.example.datastoremicroservice.model.SummaryType;

import java.util.Set;

public interface SummaryService {


    Summary get(long sensorId, Set<MeasurementType> measurementTypes, Set<SummaryType> summaryTypes);
    void handle(Data data);
}
