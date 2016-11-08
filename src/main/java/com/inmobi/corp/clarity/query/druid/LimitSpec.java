package com.inmobi.corp.clarity.query.druid;

import lombok.Data;

import java.util.List;

/**
 * Created by prathik.raj on 9/30/16.
 */

@Data
public class LimitSpec {
    List<Aggregation> aggregations;

    @Override
    public String toString() {
        String limitSpecJsonString = "\"limitSpec\": {" +
                "\"columns\": [";
        boolean first = true;
        for(Aggregation aggregation: getAggregations()) {
            if(first) {
                limitSpecJsonString += aggregation.toLimitString();
                first = false;
            } else {
                limitSpecJsonString += "," + aggregation.toLimitString();
            }
        }
        limitSpecJsonString += "], \"limit\": 500000, \"type\": \"default\"}";
        return limitSpecJsonString;
    }
}
