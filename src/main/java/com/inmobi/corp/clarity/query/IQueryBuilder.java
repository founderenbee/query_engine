package com.inmobi.corp.clarity.query;

import com.inmobi.corp.clarity.meta.MetaQuery;
import com.inmobi.corp.clarity.meta.QueryResponse;

public interface IQueryBuilder {
    void setMetaQuery(MetaQuery metaQuery);
    MetaQuery getMetaQuery();
    QueryResponse getQuery();
    QueryResponse getFormattedQuery();
}