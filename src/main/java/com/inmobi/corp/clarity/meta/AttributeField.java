package com.inmobi.corp.clarity.meta;

import lombok.Getter;
import lombok.Setter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AttributeField
{

	@Getter
	@Setter
	@JsonProperty("column")
	public String columnName;

	@Getter
	@Setter
	@JsonProperty("alias")
	public String columnAlias;

	public AttributeField() {
	}

	public AttributeField(String column, String alias){
		this.columnName = column;
		this.columnAlias = alias;
	}

	@Override
	public String toString()
	{
		return  this.columnName + " AS [" + this.columnAlias + "]";
	}
}
