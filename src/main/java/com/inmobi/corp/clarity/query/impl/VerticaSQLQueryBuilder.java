package com.inmobi.corp.clarity.query.impl;

import com.inmobi.corp.clarity.meta.MetaQuery;
import com.inmobi.corp.clarity.query.AbstractQueryBuilder;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.text.SimpleDateFormat;

public class VerticaSQLQueryBuilder  extends AbstractQueryBuilder {

    @Override
    protected void initialize() {
        super.initialize();
        this.setAliasQuoteCharacter("\"");
    }

    @Override
    public void prepareDateTimeFilter(){
        Integer fromDateAsInt =  Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(this.getMetaQuery().fromDate));
        Integer toDateAsInt =  Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(this.getMetaQuery().toDate));

        String entityId;
        String dateDimKeyColumn = "day_id";
        String factKeyColumn;
        String dateDimensionTable = "date_dim";
        String dateDimensionTableAlias;
        String dateDimensionFilterField = "date_as_int";

        if(this.getMetaQuery().getDateMode() == MetaQuery.DateMode.EVENT){
            entityId = "101";
            factKeyColumn = "event_day_id";
            dateDimensionTableAlias = "evdt";
        }
        else {
            entityId = "100";
            factKeyColumn = "request_day_id";
            dateDimensionTableAlias = "rqdt";
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
