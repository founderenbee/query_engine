package com.inmobi.corp.clarity.meta;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.inmobi.corp.clarity.query.IQueryBuilder;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@lombok.Getter
@lombok.Setter
@Service
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetaQuery
{

    private static ApplicationContext applicationContext = new ClassPathXmlApplicationContext("applicationContext.xml");
	private static Logger log = LoggerFactory.getLogger(MetaQuery.class.getName());

    @JsonProperty("dateMode")
    public DateMode dateMode = DateMode.EVENT;

    @JsonProperty("fromDate")
    public Timestamp fromDate = null;

    @JsonProperty("toDate")
    public Timestamp toDate = null;

	@JsonProperty("table")
	public String factTable = null;

	@JsonProperty("attributes")
	public List<AttributeField> attributes = null;

	@JsonProperty("measures")
	public List<AttributeField> measures = null;

	@JsonProperty("filters")
	public List<Filter> filters = new ArrayList<>();

    @JsonInclude(JsonInclude.Include.NON_NULL)
	@JsonProperty("orderBy")
	public OrderField[] orderBy = null;

	@JsonProperty("limit")
	public int limit = 0;

	@JsonProperty("offset")
	public int offset = 0;

    @JsonProperty("dialect")
    public Dialect dialect = Dialect.VOLTDB;

	@JsonProperty("queryType")
	public QueryType queryType = QueryType.META_JSON;

    @JsonProperty("queryTemplate")
    @JsonIgnore
    public String queryTemplate = null;

    public enum Dialect {
        VOLTDB, VERTICA, LENS, DRUID
	}

    public enum QueryType {
        META_JSON, TEMPLATE
    }

    public enum DateMode {
        REQUEST, EVENT
    }

    @Override
	public String toString()
	{
		String signature = "MetaQuery to : \nSELECT\n";

		if (this.attributes != null)
		{
            for (AttributeField attribute : this.getAttributes()) {
                signature = signature + attribute.toString();
            }
		}

		if (this.measures != null)
		{
            for (AttributeField measure : this.measures) {
                signature = signature + measure.toString();
            }
		}

        signature = signature + "\n FROM X_DATASOURCE";
		signature = signature + "\n WHERE DATE BETWEEN " + this.fromDate.toString() + " AND " + this.toDate.toString();

		if (this.filters != null)
		{
            for (Object filter : filters) {
                signature = signature + filter.toString();
            }
		}

        if (this.orderBy != null)
        {
            signature = signature + "\n ORDER BY ";
            for (OrderField orderField : orderBy) {
                signature = signature + orderField.toString();
            }
        }

		return signature;
	}

	@Override
	public int hashCode()
	{
		return this.toString().hashCode();
	}

	@JsonIgnore
	public String getAttributeColumns()
	{
		List<String> attr = new ArrayList<>();
		if (this.attributes != null && this.attributes.size() > 0){
            attr.addAll(this.attributes.stream().map(attribute -> "'" + attribute.columnName + "'").collect(Collectors.toList()));
            return String.join(",",attr);
        }
        else {
            return "'_'";
        }
	}

	@JsonIgnore
	public String getMeasureColumns()
	{
		List<String> msr = new ArrayList<>();
		if (this.measures != null && this.measures.size() > 0)
        {
            msr.addAll(this.measures.stream().map(measure -> "'" + measure.columnName + "'").collect(Collectors.toList()));
            return String.join(",", msr);
        }
        else {
            return "'_'";
        }
	}

	@JsonIgnore
	public String getFilterColumns()
	{
		List<String> fltr = new ArrayList<>();
        if (this.filters != null && this.filters.size() > 0)
        {
            fltr.addAll(this.filters.stream().map(filter -> filter.objFilter.getFilterColumns()).collect(Collectors.toList()));
            return String.join(",", fltr);
        }
        else {
            return "'_'";
        }
	}

	@JsonIgnore
	public boolean validate()
	{
		if (this.fromDate == null || this.toDate == null)
		{
			log.debug("Invalid Date : " + this.fromDate + " | " + this.toDate);
			return false;
		}
        return !(this.getAttributeColumns().matches("(?i)[^a-zA-Z0-9_', ]*") || this.getMeasureColumns().matches("(?i)[^a-zA-Z0-9_', ]*") || this.getFilterColumns().matches("(?i)[^a-zA-Z0-9_', ]*"));
    }

    @JsonIgnore
    public QueryResponse fetchFinalQuery(boolean prettyFormatQuery ){
        IQueryBuilder queryBuilder = (IQueryBuilder) applicationContext.getBean(this.dialect.name());
        queryBuilder.setMetaQuery(this);
        if(this.validate()){
            return (prettyFormatQuery)? queryBuilder.getFormattedQuery() : queryBuilder.getQuery();
        }
        else {
            QueryResponse queryResponse = new QueryResponse();
            queryResponse.setError("Invalid characters found for JSON attribute values ( (?i)[^a-zA-Z0-9_', ]* ) !");
            return queryResponse;
        }
    }
}