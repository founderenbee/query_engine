package com.inmobi.corp.clarity.meta;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Getter;
import lombok.Setter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonDeserialize(as = SimpleFilter.class)
public class SimpleFilter implements IFilter
{

	@Getter
	@Setter
	@JsonProperty("filterColumn")
	public String filterColumn = null;

	@Getter
	@Setter
	@JsonProperty("filterValue")
	public List<String> filterValue = null;

	@Getter
	@Setter
	@JsonProperty("operator")
	public Operator operator = Operator.IN;

	public SimpleFilter() {
	}

	/**
	 * @param filterColumn
	 * @param filterValue
	 * @param Operator
	 */
	public SimpleFilter(String filterColumn, List<String> filterValue, Operator Operator) {
		this.filterColumn = filterColumn;
		this.filterValue = filterValue;
		this.operator = Operator;
	}

    @Override
    public String toString() {
        return "SimpleFilter{" +
                "column='" + filterColumn + '\'' +
                ", value=" + String.join(",", filterValue) +
                ", operator=" + operator +
                '}';
    }

	/**
	 * @return
	 */
	@Override
	@JsonIgnore
    public String getFilterColumns() {
        return "'" + this.filterColumn + "'";
    }

    public static enum Operator {
		LIKE, NOT_LIKE, EQUALS, NOT_EQUALS, IN, NOT_IN, GREATER, GREATER_OR_EQUAL, LESS, LESS_OR_EQUAL, LIKE_IGNORE_CASE, NOT_LIKE_IGNORE_CASE, BETWEEN;
	}
}