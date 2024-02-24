package com.example.datastoremicroservice.web.mapper;

import com.example.datastoremicroservice.model.Summary;
import com.example.datastoremicroservice.web.dto.SummaryDto;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring") // таким образом маппер может использоваться во время с
// борки и для него будет создан класс в котором реализован маппинг
public interface SummaryMapper extends Mappable<Summary, SummaryDto> {
}
