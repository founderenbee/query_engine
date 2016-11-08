package com.inmobi.corp.clarity.query.druid;

import lombok.Data;

/**
 * Created by prathik.raj on 9/29/16.
 */

@Data
public class Aggregation {
    private final String name;
    private final String dataTypeName;
    private final String alias;

    String aggregationQuery() {
        return String.format("{ \"name\": \"%s\", \"type\": \"%s\", \"fieldName\": \"%s\" }",
                getAlias(),
                getDataTypeName(),
                getName());
    }

    String toLimitString() {
        return "{" +
                String.format("\"dimension\": \"%s\",", getAlias()) +
                "\"direction\": \"descending\"" +
                "}";
    }
}
