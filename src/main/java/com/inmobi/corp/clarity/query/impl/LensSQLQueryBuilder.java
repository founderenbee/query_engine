package com.inmobi.corp.clarity.query.impl;

import com.inmobi.corp.clarity.meta.MetaQuery;
import com.inmobi.corp.clarity.query.AbstractQueryBuilder;
import org.jooq.Condition;
import org.jooq.impl.DSL;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class LensSQLQueryBuilder extends AbstractQueryBuilder {

    @Override
    public void prepareJoins() {
        this.getKeyColumnsList().add("dimension_id");
    }

    @Override
    public void addLimitandOffset() {
        if(this.getMetaQuery().getLimit() > 0) {
            this.getSelectQuery().addLimit(DSL.inline(this.getMetaQuery().getLimit()));
        }
    }


    @Override
    public void prepareDateTimeFilter(){

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(this.getMetaQuery().getToDate());
        if(calendar.get(Calendar.HOUR) == 0) {
            calendar.add(Calendar.DAY_OF_WEEK, 1);
            this.getMetaQuery().getToDate().setTime(calendar.getTime().getTime());
        }

        String fromDate = new SimpleDateFormat("yyyy-MM-dd-HH").format(this.getMetaQuery().fromDate);
        String toDate =  new SimpleDateFormat("yyyy-MM-dd-HH").format(this.getMetaQuery().getToDate());

        Condition condition;

        if(this.getMetaQuery().getDateMode() == MetaQuery.DateMode.EVENT){
            // 'time_range_in(event_time, '2016-08-22-00', '2016-08-23-00')';
            condition = DSL.condition("time_range_in(event_time, {0}, {1})",fromDate, toDate );
        }
        else {
            condition = DSL.condition("time_range_in(request_time, {0}, {1} )", fromDate, toDate );
        }
        if(this.isHasDerivedMeasures()) {
            this.getInnerSelectQuery().addConditions(condition);
        }
        else {
            this.getSelectQuery().addConditions(condition);
        }
    }
}
