package com.inmobi.corp.clarity.meta;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ComplexFilter implements IFilter {

    @Getter
    @Setter
    @JsonProperty("qualifier")
    public Qualifier qualifier = Qualifier.AND;

    @Getter
    @Setter
    @JsonProperty("filters")
    public List<Filter> filters = null;

    public ComplexFilter() {
    }

    @Override
    public String toString() {
        return "ComplexFilter{" +
                "qualifier=" + qualifier +
                ", filters=" + filters +
                '}';
    }

    @Override
    public String getFilterColumns() {
        List<String> columnList = this.filters.stream().map(f -> f.objFilter.getFilterColumns()).collect(Collectors.toList());
        return String.join(",", columnList);
    }

    public enum Qualifier
    {
        AND, OR
    }
}