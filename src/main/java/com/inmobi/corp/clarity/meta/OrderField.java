package com.inmobi.corp.clarity.meta;

import lombok.Getter;
import lombok.Setter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class OrderField
{
	@Getter
	@Setter
	@JsonProperty("columnName")
	public String columnName;

	@Getter
	@Setter
	@JsonProperty("orderBy")
	public Order orderBy;

	public OrderField(){}

	public OrderField(String columnName, Order orderBy){
		this.columnName = columnName;
		this.orderBy = orderBy;
	}

	@Override
	public String toString()
	{
		return  this.columnName + " " + this.orderBy + ", ";
	}

	public enum Order{
		ASC, DESC
	}
}
