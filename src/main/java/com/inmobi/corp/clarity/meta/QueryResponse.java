package com.inmobi.corp.clarity.meta;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.stereotype.Service;

import java.util.ArrayList;

@lombok.Getter
@lombok.Setter
@Service
public class QueryResponse {

    @JsonProperty("data")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public ArrayList<Object> data = new ArrayList<>();

    @JsonProperty("queryCount")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public Integer queryCount = 0;

    @JsonProperty("queryList")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public ArrayList<String> queryList = new ArrayList<>();

    @JsonProperty("joinColumns")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String joinColumns = "";

    @JsonProperty("derivedColumns")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public ArrayList<DerivedColumn> derivedColumns = new ArrayList<>();

    @JsonProperty("error")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String error = "";

    @JsonProperty("warning")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String warning = "";

    private class DerivedColumn {
        public String derivedColumnName = "";
        public String formulae = "";
    }

}