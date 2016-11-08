package com.inmobi.corp.clarity.query.impl;

import com.inmobi.corp.clarity.dao.QBMetaDao;
import com.inmobi.corp.clarity.meta.*;
import com.inmobi.corp.clarity.query.IQueryBuilder;
import com.inmobi.corp.clarity.query.druid.Aggregation;
import com.inmobi.corp.clarity.query.druid.DimensionSpec;
import com.inmobi.corp.clarity.query.druid.GroupBy;
import com.inmobi.corp.clarity.utils.FilterUtil;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by prathik.raj on 8/30/16.
 */
public class DruidQueryBuilder implements IQueryBuilder {

    @Getter
    List<DimensionSpec> dimensions = new LinkedList<>();

    @Autowired(required = true)
    @Getter(AccessLevel.PRIVATE)
    protected QBMetaDao qbMetaDao;

    @Setter
    @Getter
    MetaQuery metaQuery;

    @Getter
    QueryResponse queryResponse = new QueryResponse();

    @Getter
    List<Aggregation> aggregations = new LinkedList<>();

    @Getter
    HashMap<String, String> postAggregations = new HashMap<>();

    List<HashMap<String, String>> measuresList;

    @Getter
    String joinColumns;


    public void setAggregations(List<HashMap<String, String>> measuresList) {
        for(HashMap<String, String> measure: measuresList) {
            if(!measure.get("is_derived_column").equals("Y")) {
                System.out.println("measure.get(\"COLUMN_NAME\") = " + measure.get("COLUMN_NAME"));
                Aggregation aggregation = new Aggregation(measure.get("COLUMN_NAME"),
                        measure.get("data_type_name"),
                        measure.get("unique_column_name"));
                getAggregations().add(aggregation);
            } else {
                Aggregation aggregation = new Aggregation(
                        measure.get("COLUMN_NAME"),
                        measure.get("data_type_name"),
                        measure.get("variable_as_column_alias"));
                getAggregations().add(aggregation);
                getPostAggregations().put(measure.get("unique_column_name"),
                        measure.get("derived_column_formula"));
            }
        }
    }

    private void setDimensions(List<HashMap<String, String>> attributeList) {
        joinColumns = null;
        for(HashMap<String, String> attribute: attributeList) {
            DimensionSpec dimensionSpec = new DimensionSpec(attribute.get("COLUMN_NAME"),
                    attribute.get("UNIQUE_COLUMN_NAME"));

            if(joinColumns == null) {
                joinColumns = attribute.get("UNIQUE_COLUMN_NAME");
            } else {
                joinColumns += "," + attribute.get("UNIQUE_COLUMN_NAME");
            }

            getDimensions().add(dimensionSpec);
        }
    }

    private List<HashMap<String, String>> getDimensionsList() throws SQLException {
        return qbMetaDao.getSelectFieldsMetadata("DRUID",
                 metaQuery.getAttributeColumns());
    }

    private List<HashMap<String, String>> getMeasuresList() throws SQLException {
        if(measuresList == null) {
            measuresList = qbMetaDao.getMeasureFieldsMetaData("DRUID",
                    metaQuery.getMeasureColumns());
        }
        return measuresList;
    }

    private String getDatasource() {
        /*List<HashMap<String,String>> factTablesWithAllRequiredDimensions = this.getQbMetaDao().getFactTablesWithAllRequiredDimensions(
                this.getMetaQuery().getDialect().name(),
                this.getKeyColumnsList().stream().map(addQuote).collect(Collectors.joining(",")),
                this.getKeyColumnsList().size());*/
        // TODO: Generalize
        return "demand";
    }

    @Override
    public QueryResponse getFormattedQuery() {
        return getQuery();
    }

    @Override
    public QueryResponse getQuery() {


        try {
            setDimensions(getDimensionsList());

            if(getMeasuresList().size() > 0) {
                setAggregations(getMeasuresList());
            } else {
                getQueryResponse().setQueryCount(0);
                getQueryResponse().setError(String.format("Druid does not support dimension" +
                        " only query, measures passed: %s", metaQuery.getMeasureColumns()));
                return getQueryResponse();
            }

            HashMap<String, HashMap<String, String>> filterMetadata
                    = new HashMap<>();

            List<HashMap<String,String>> filterColumnDetails
                    = this.getQbMetaDao().getFilterColumnDetails(
                        this.getMetaQuery().getDialect().name(),
                        this.getMetaQuery().getFilterColumns());

            for(HashMap<String, String> rs : filterColumnDetails) {
                filterMetadata.put(rs.get("UNIQUE_COLUMN_NAME"), rs);
            }

            GroupBy groupBy = new GroupBy();
            groupBy.setDataSource(getDatasource());
            groupBy.setPostAggregation(getPostAggregations());
            groupBy.setAggregations(getAggregations());
            groupBy.setDimensions(getDimensions());
            if(metaQuery.getFilters().size() > 0) {
                if(metaQuery.getFilters().size() > 1) {
                    groupBy.setFilters(FilterUtil.buildComplexFilter(metaQuery.getFilters(),
                            ComplexFilter.Qualifier.AND), filterMetadata);
                } else {
                    groupBy.setFilters(metaQuery.getFilters().get(0), filterMetadata);
                }
            }
            groupBy.setStartPeriod(getMetaQuery().getFromDate().toString().replace(" ", "T"));
            groupBy.setEndPeriod(getMetaQuery().getToDate().toString().replace(" ", "T"));
            getQueryResponse().getQueryList().add(groupBy.toString());
            getQueryResponse().setJoinColumns(getJoinColumns());
            getQueryResponse().setQueryCount(1);

        } catch (Exception e) {
            getQueryResponse().setError(e.getMessage());
        }

        return getQueryResponse();
    }
}
