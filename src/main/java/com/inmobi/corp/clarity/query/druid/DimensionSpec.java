package com.inmobi.corp.clarity.query.druid;

import lombok.Data;

/**
 * Created by prathik.raj on 9/29/16.
 */


@Data
public class DimensionSpec {
    private final String name;
    private final String alias;

    @Override
    public String toString() {
        return String.format("{ \"type\" : \"default\", \"dimension\" : \"%s\", \"outputName\": \"%s\" }",
                getName(),
                getAlias());
    }
}
