package com.inmobi.corp.clarity.query.druid;

import com.inmobi.corp.clarity.meta.ComplexFilter;
import com.inmobi.corp.clarity.meta.Filter;
import com.inmobi.corp.clarity.meta.SimpleFilter;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by prathik.raj on 8/31/16.
 */
public abstract class AbstractQuery {
    @Setter
    @Getter
    String queryType;

    @Getter
    @Setter
    String dataSource;

    @Setter
    @Getter
    String granularity;

    @Setter
    @Getter
    String intervals;

    @Getter
    @Setter
    List<Aggregation> aggregations;

    @Setter
    @Getter
    String postAggregations;

    @Setter
    @Getter
    String startPeriod;

    @Setter
    @Getter
    String endPeriod;

    @Getter
    String filters;


    private String simpleFilterJSON(Filter filter,
                                    HashMap<String, HashMap<String, String>> filterMeta) {
        SimpleFilter simpleFilter = (SimpleFilter) filter.getObjFilter();
        String dimension = filterMeta.get(simpleFilter.getFilterColumn()).get(
                "COLUMN_NAME");
        String values = "";
        List<String> individualFilters = simpleFilter.getFilterValue().stream().map(value -> String.format("{" +
                "\"type\": \"selector\"," +
                "\"dimension\": \"%s\"," +
                "\"value\": \"%s\"" +
                "}", dimension, value)).collect(Collectors.toList());

        boolean first = true;
        for(String singleFilter: individualFilters) {
            if(first) {
                values += singleFilter;
                first = false;
            } else {
                values += "," + singleFilter;
            }
        }

        return "{\n" +
                "    \"type\": \"or\",\n" +
                String.format("    \"fields\": [%s]\n", values) +
                "}\n";

    }

    private String buildFilters(Filter filter,
                                HashMap<String, HashMap<String, String>> filterMeta) {

        if(filter.getType() == Filter.FilterType.simple) {
            return simpleFilterJSON(filter, filterMeta);
        } else {
            ComplexFilter complexFilter = (ComplexFilter) filter.getObjFilter();
            List<String> subFilters = complexFilter.getFilters().stream().map(
                    subF -> buildFilters(subF, filterMeta)).collect(Collectors.toList());
            String filterString = null;

            for(String subF: subFilters) {
                if(filterString == null) {
                    filterString = subF;
                } else {
                    filterString += "," + subF;
                }
            }
            String filterQuery = "{ \"type\": \"%s\", \"fields\": " +
                    "["+filterString + "] }";
            if(complexFilter.getQualifier() == ComplexFilter.Qualifier.AND) {
               filterQuery = String.format(filterQuery, "and");
            } else {
                filterQuery = String.format(filterQuery, "or");
            }

            return filterQuery;
        }
    }

    public void setFilters(Filter filter,
                           HashMap<String, HashMap<String, String>> filterMeta) {
        if(filter == null || filterMeta == null) {
            return;
        }

        this.filters = buildFilters(filter, filterMeta);
    }

    String buildAggregationQuery() {
        StringBuilder aggregationJson = new StringBuilder();
        aggregationJson.append("[");
        boolean first = true;
        for(Aggregation aggregation: getAggregations()) {
            if(first) {
                aggregationJson.append(aggregation.aggregationQuery());
                first = false;
            } else {
                aggregationJson.append(String.format(",%s",
                        aggregation.aggregationQuery()));
            }
        }
        aggregationJson.append("]");
        return aggregationJson.toString();
    }

    public void setPostAggregation(Map<String, String> postAggregationMap) {
        boolean first = true;
        postAggregations = "";
        for(String postAgg: postAggregationMap.values()) {
            if(first) {
                postAggregations += postAgg;
                first = false;
            } else {
                postAggregations += "," + postAgg;
            }
        }
    }
}
