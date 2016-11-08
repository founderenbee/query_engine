package com.inmobi.corp.clarity.query;

import org.jooq.SQLDialect;
import org.jooq.SelectQuery;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;

class DSLFactory {
    static SelectQuery<?> getSelectQuery(SQLDialect sqlDialect){
        Settings settings = new Settings().withRenderFormatted(true);
        return DSL.using(sqlDialect, settings).selectQuery();
    }
}
