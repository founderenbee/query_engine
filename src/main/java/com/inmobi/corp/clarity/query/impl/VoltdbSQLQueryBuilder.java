package com.inmobi.corp.clarity.query.impl;

import com.inmobi.corp.clarity.meta.MetaQuery;
import com.inmobi.corp.clarity.query.AbstractQueryBuilder;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.text.SimpleDateFormat;

public class VoltdbSQLQueryBuilder extends AbstractQueryBuilder {
    @Override
    public void prepareDateTimeFilter(){
        Integer fromDateAsInt =  Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(this.getMetaQuery().fromDate));
        Integer toDateAsInt =  Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(this.getMetaQuery().toDate));

        String entityId;
        String dateDimKeyColumn = "DAY_ID";
        String factKeyColumn;
        String dateDimensionTable = "DATE_DIM";
        String dateDimensionTableAlias;
        String dateDimensionFilterField = "DATE_AS_INT";

        if(this.getMetaQuery().getDateMode() == MetaQuery.DateMode.EVENT){
            entityId = "2";
            factKeyColumn = "EVENT_DAY_ID";
            dateDimensionTableAlias = "EVENT";
        }
        else {
            entityId = "3";
            factKeyColumn = "REQUEST_DAY_ID";
            dateDimensionTableAlias = "REQ";
        }

        Table<?> timeDimension = this.tablesCatalog.get(entityId);
        Field<Integer> leftField = DSL.field( dateDimensionTableAlias + "." + dateDimKeyColumn, Integer.class);
        Field<Integer> rightField = DSL.field( "fact." + factKeyColumn, Integer.class);
        Field<Integer> filterColumn = DSL.field( dateDimensionTableAlias + "." + dateDimensionFilterField, Integer.class);
        Condition joinCondition = leftField.equal(rightField);
        Condition filterCondition = filterColumn.between(DSL.inline(fromDateAsInt), DSL.inline(toDateAsInt));
        if(timeDimension == null){
            timeDimension = DSL.table(dateDimensionTable).as(dateDimensionTableAlias);
            this.tablesCatalog.put(entityId, timeDimension);
            joinCondition = joinCondition.and(filterCondition);
            this.getSelectQuery().addJoin(timeDimension, joinCondition);
        } else {
            this.getSelectQuery().addConditions(filterCondition);
        }
        this.getKeyColumnsList().add(factKeyColumn);
    }
}