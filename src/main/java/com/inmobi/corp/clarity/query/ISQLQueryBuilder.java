package com.inmobi.corp.clarity.query;


public interface ISQLQueryBuilder extends IQueryBuilder {
    void  prepareDateTimeFilter();
    void  prepareSelect();
    void  prepareSelectMeasures();
    void  prepareFrom();
    void  prepareJoins();
    void  prepareFilters();
    void  addOrderBy();
    void  addLimitandOffset();
}
