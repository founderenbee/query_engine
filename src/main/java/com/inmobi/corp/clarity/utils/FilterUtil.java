package com.inmobi.corp.clarity.utils;

import com.inmobi.corp.clarity.meta.ComplexFilter;
import com.inmobi.corp.clarity.meta.Filter;

import java.util.List;

/**
 * Created by prathik.raj on 10/6/16.
 */
public class FilterUtil {
    public static Filter buildComplexFilter(List<Filter> filters, ComplexFilter.Qualifier qualifier) {
        Filter complexFilter = new Filter();
        complexFilter.setType(Filter.FilterType.complex);
        ComplexFilter complexFilterObj = new ComplexFilter();
        complexFilterObj.setQualifier(qualifier);
        complexFilterObj.setFilters(filters);
        complexFilter.setObjFilter(complexFilterObj);

        return complexFilter;
    }
}
