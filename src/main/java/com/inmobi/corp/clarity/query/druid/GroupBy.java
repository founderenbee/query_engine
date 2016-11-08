package com.inmobi.corp.clarity.query.druid;

import lombok.Getter;

import java.util.List;
import java.util.Set;

/**
 * Created by prathik.raj on 8/31/16.
 */
public class GroupBy extends AbstractQuery {

    @Getter
    List<DimensionSpec> dimensions;

    public void setDimensions(List<DimensionSpec> dimensions) {
        this.dimensions = dimensions;
    }

    public GroupBy() {
        super();
        setQueryType("groupBy");
    }

    @Override
    public String toString() {
        StringBuilder queryBuilder = new StringBuilder();
        LimitSpec limitSpec = new LimitSpec();
        limitSpec.setAggregations(getAggregations());
        queryBuilder.append("{\n").append(String.format("  \"queryType\": \"%s\",\n", getQueryType()))
                .append(String.format("  \"dataSource\": \"%s\",\n", getDataSource()))
                .append("  \"granularity\": \"all\",\n")
                .append(String.format("  \"dimensions\": %s,\n", getDimensions()))
                .append(String.format(" \"filter\": %s,\n", getFilters()))
                .append(String.format("  \"aggregations\": %s,\n", buildAggregationQuery()))
                .append(String.format("  \"intervals\": [ \"%s/%s\" ]," +
                        "" +
                        "\n", getStartPeriod(), getEndPeriod()))
                .append(String.format("  \"postAggregations\": [%s],\n",
                        getPostAggregations()))
                .append(limitSpec.toString())
                .append("}");
        return queryBuilder.toString();
    }
}
