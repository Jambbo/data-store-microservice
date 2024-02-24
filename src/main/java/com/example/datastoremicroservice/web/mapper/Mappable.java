package com.example.datastoremicroservice.web.mapper;

import java.util.List;

public interface Mappable<E,D> {

    E toEntity(D dto);
    List<E> toEntity(List<D> dto);
    D toDto(E entity);
    List<D> toDto(List<D> entity);

}
