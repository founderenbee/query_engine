package com.inmobi.corp.clarity.meta;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

public class Filter {

    @Getter
    @Setter
    @JsonProperty("type")
    public  FilterType type = FilterType.simple;

    @Getter
    @Setter
    @JsonProperty("obj")
    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.EXTERNAL_PROPERTY,
            property = "type"
    )
    @JsonSubTypes({
            @JsonSubTypes.Type(value = SimpleFilter.class, name = "simple"),
            @JsonSubTypes.Type(value = ComplexFilter.class, name = "complex")
    })

    public IFilter objFilter = null;

    public enum FilterType {
        simple, complex
    }
}
