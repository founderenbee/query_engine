package com.inmobi.corp.clarity.query;

import com.inmobi.corp.clarity.dao.QBMetaDao;
import com.inmobi.corp.clarity.meta.*;
import com.inmobi.corp.clarity.utils.StackTraceUtil;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.jooq.*;
import org.jooq.Comparator;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
@ToString(callSuper=true)
public abstract class AbstractQueryBuilder implements ISQLQueryBuilder {

    @Autowired()
    @Getter(AccessLevel.PRIVATE)
    protected QBMetaDao qbMetaDao;

    @Getter
    private QueryResponse queryResponse = new QueryResponse();

    @Getter
    @Setter
    protected boolean isDimensionOnlyQuery = false;

    @Getter
    @Setter
    protected boolean hasDerivedMeasures = false;

    @Getter
    @Setter
    protected boolean isMultiFactQuery = false;

    @Getter
    @Setter
    protected String aliasQuoteCharacter = "`";


    @Getter
    protected HashMap<String, List<AttributeField>> selectedFactTables = new HashMap<>();

    @Getter
    @Setter
    protected boolean enablePrettyFormatting = false;

    @Getter
    public static final Set<String> numberTypeNames = new HashSet<>(Arrays.asList("FLOAT", "INTEGER", "TINYINT", "SMALLINT", "BIGINT", "int", "numeric"));
    @Getter
    public static final Set<SimpleFilter.Operator> multiValueOperators = new HashSet<>(Arrays.asList(SimpleFilter.Operator.IN, SimpleFilter.Operator.NOT_IN));
    @Getter
    public static final Set<SimpleFilter.Operator> singleValueOperators = new HashSet<>(Arrays.asList(
            SimpleFilter.Operator.LIKE,
            SimpleFilter.Operator.NOT_LIKE,
            SimpleFilter.Operator.EQUALS,
            SimpleFilter.Operator.NOT_EQUALS,
            SimpleFilter.Operator.GREATER,
            SimpleFilter.Operator.GREATER_OR_EQUAL,
            SimpleFilter.Operator.LESS,
            SimpleFilter.Operator.LESS_OR_EQUAL,
            SimpleFilter.Operator.LIKE_IGNORE_CASE,
            SimpleFilter.Operator.NOT_LIKE_IGNORE_CASE));
    @Getter
    public static final Set<SimpleFilter.Operator> dualValueOperators = new HashSet<>(Collections.singletonList(SimpleFilter.Operator.BETWEEN));

    @Getter
    protected Hashtable<String, Table<?>> tablesCatalog = new Hashtable<>();

    @lombok.Getter
    @lombok.Setter
    protected MetaQuery metaQuery;

    @Getter
    @Setter
    protected String entityIds = null;

    @Getter
    protected Set<String> keyColumnsList = new HashSet<>();

    private void setSelectQuery(SelectQuery<?> selectQuery) {
        if(this.selectQuery != null){
            throw new IllegalStateException("setSelectQuery has been called!");
        }
        this.selectQuery = selectQuery;
    }

    @Getter
    protected SelectQuery<?> selectQuery;

    private void setInnerSelectQuery(SelectQuery<?> selectQuery) {
        if(this.innerSelectQuery != null){
            throw new IllegalStateException("setSelectQuery has been called!");
        }
        this.innerSelectQuery = selectQuery;
    }

    @Getter
    protected SelectQuery<?> innerSelectQuery;

    @Getter
    protected Table<?> factTable = DSL.table("__fACT").as("fact");

    Function<String, String> addQuote = s -> {
        String quoteChar = "'";
        return quoteChar + s + quoteChar;
    };


    public AbstractQueryBuilder() {
    }

    protected void initialize(){
        this.setSelectQuery(DSLFactory.getSelectQuery(SQLDialect.MYSQL));
        this.getSelectQuery().addFrom(this.getFactTable());
        this.setInnerSelectQuery(DSLFactory.getSelectQuery(SQLDialect.MYSQL));
        this.getInnerSelectQuery().addFrom(this.getFactTable());
    }

    @Override
    public abstract void prepareDateTimeFilter();

    @Override
    public void prepareJoins() {
        this.prepareJoinsRecursive(this.getEntityIds());
    }

    private void prepareJoinsRecursive(String strEntityIds) {
        try {
            List<HashMap<String, String>> joinChainMetadata = this.getQbMetaDao().getJoinChainMetadata(strEntityIds);
            for(HashMap<String, String> rs : joinChainMetadata){
                if(!rs.get("ALIAS").equalsIgnoreCase("fact")) {
                    if (rs.get("PARENT_ENTITY_ID").equalsIgnoreCase("-99")) {
                        Table<?> joinTable = this.getTablesCatalog().get(rs.get("ENTITY_ID"));
                        if (joinTable == null) {
                            joinTable = DSL.table(rs.get("ENTITY_NAME")).as(rs.get("ALIAS"));
                            this.getTablesCatalog().put(rs.get("ENTITY_ID"), joinTable);
                            Field<String> leftField = DSL.field("fact." + rs.get("JOIN_KEY_COLUMN"), String.class);
                            Field<String> rightField = DSL.field(rs.get("ALIAS") + "." + rs.get("KEY_COLUMN"), String.class);
                            Condition joinCondition = leftField.equal(rightField);
                            this.getSelectQuery().addJoin(joinTable, JoinType.JOIN, joinCondition);
                            this.getKeyColumnsList().add(rs.get("JOIN_KEY_COLUMN"));
                        }
                    } else {
                        if (!rs.get("P_PARENT_ENTITY_ID").equalsIgnoreCase("-99")) {
                            this.prepareJoinsRecursive(rs.get("P_PARENT_ENTITY_ID"));
                        }
                        // Parent Join
                        // Table<?> parent_of_parentTable = this.getTablesCatalog().get(rs.get("P_PARENT_ENTITY_ID"));
                        Table<?> parentTable = this.getTablesCatalog().get(rs.get("PARENT_ENTITY_ID"));
                        Field<String> leftField;
                        Field<String> rightField;
                        if (parentTable == null) {
                            parentTable = DSL.table(rs.get("P_ENTITY_NAME")).as(rs.get("P_ALIAS"));
                            this.getTablesCatalog().put(rs.get("PARENT_ENTITY_ID"), parentTable);
                            String ppAlias = (!rs.get("P_PARENT_ENTITY_ID").equalsIgnoreCase("-99")) ? this.getTablesCatalog().get(rs.get("P_PARENT_ENTITY_ID")).getName() : "fact";
                            leftField = DSL.field(ppAlias + "." + rs.get("P_JOIN_KEY_COLUMN"), String.class);
                            rightField = DSL.field(rs.get("P_ALIAS") + "." + rs.get("P_KEY_COLUMN"), String.class);
                            Condition parentJoinCondition = leftField.equal(rightField);
                            this.getSelectQuery().addJoin(parentTable, JoinType.JOIN, parentJoinCondition);
                            if (rs.get("P_PARENT_ENTITY_ID").equalsIgnoreCase("-99")) {
                                this.getKeyColumnsList().add(rs.get("P_JOIN_KEY_COLUMN"));
                            }
                        }
                        // Child Join
                        Table<?> childTable = this.getTablesCatalog().get(rs.get("ENTITY_ID"));
                        if (childTable == null) {
                            childTable = DSL.table(rs.get("ENTITY_NAME")).as(rs.get("ALIAS"));
                            this.getTablesCatalog().put(rs.get("ENTITY_ID"), childTable);
                            leftField = DSL.field(rs.get("P_ALIAS") + "." + rs.get("JOIN_KEY_COLUMN"), String.class);
                            rightField = DSL.field(rs.get("ALIAS") + "." + rs.get("KEY_COLUMN"), String.class);
                            Condition childJoinCondition = leftField.equal(rightField);
                            this.getSelectQuery().addJoin(childTable, JoinType.JOIN, childJoinCondition);
                        }
                    }
                }
                else {
                    this.getKeyColumnsList().add(rs.get("JOIN_KEY_COLUMN"));
                }
            }
        } catch (SQLException e) {
            this.getQueryResponse().setError( this.getQueryResponse().getError() + e.getMessage() + StackTraceUtil.stackTraceToString(e));
        }
    }

    @Override
    public void prepareSelect() {
        try {
            List<HashMap<String,String>> selectFieldsMetadata = this.getQbMetaDao().getSelectFieldsMetadata(this.getMetaQuery().getDialect().name(), this.getMetaQuery().getAttributeColumns());
            for(HashMap<String, String> rs : selectFieldsMetadata){
                String alias = (rs.get("ALIAS") != null && rs.get("ALIAS").length() > 0)?rs.get("ALIAS") + ".":"";
                SelectField<Object> selectField = DSL.field( alias + rs.get("COLUMN_NAME")).as(rs.get("UNIQUE_COLUMN_NAME"));
                SelectField<Object> innerSelectField = DSL.field( alias + rs.get("COLUMN_NAME")).as(rs.get("COLUMN_NAME"));
                GroupField groupField = DSL.field( alias + rs.get("COLUMN_NAME"));
                this.getSelectQuery().addSelect(selectField);
                this.getInnerSelectQuery().addSelect(innerSelectField);
                if( !this.metaQuery.getDialect().equals(MetaQuery.Dialect.LENS) && !this.isDimensionOnlyQuery()) {
                    this.getSelectQuery().addGroupBy(groupField);
                    this.getInnerSelectQuery().addGroupBy(groupField);
                }
            }
        } catch (SQLException e) {
            this.getQueryResponse().setError( this.getQueryResponse().getError() + e.getMessage() + StackTraceUtil.stackTraceToString(e));
        }
    }

    @Override
    public void prepareSelectMeasures() {

        int i = 1;
        if(! this.isDimensionOnlyQuery() && !this.isMultiFactQuery()){
            this.addOrderBy();
            this.addLimitandOffset();
        }
        else {
            this.getQueryResponse().setWarning(this.getQueryResponse().getWarning() + " Order by & Limit clauses omitted due to multi-fact query.. ! query execution provider should honor it.");
        }

        for (Object o : this.getSelectedFactTables().entrySet()) {
            Map.Entry element = (Map.Entry) o;
            List<Collection<Field<BigDecimal>>>    partialMeasuresCollectionList = preparePartialMeasuresList(this.getSelectedFactTables().get(element.getKey()));
            this.getSelectQuery().addSelect(partialMeasuresCollectionList.get(0));
            this.getInnerSelectQuery().addSelect(partialMeasuresCollectionList.get(1));
            String factAlias = (this.isDimensionOnlyQuery() )? "" : " fact";
            if(this.isHasDerivedMeasures()){
                String innerQuery = this.getInnerSelectQuery().getSQL(ParamType.INLINED)
                                        .replace(" as `fact`", factAlias)
                                        .replace("`", this.getAliasQuoteCharacter())
                                        .replace("__fACT", element.getKey().toString());
                String outerQuery = this.getSelectQuery().getSQL(ParamType.INLINED)
                                        .replace(" as `fact`", factAlias)
                                        .replace("`", this.getAliasQuoteCharacter())
                                        .replace("__fACT", " ( " + innerQuery + " ) ");
                this.getQueryResponse().getQueryList().add(outerQuery);
                this.getQueryResponse().setQueryCount(i++);
            }
            else {
                String query = this.getSelectQuery().getSQL(ParamType.INLINED)
                        .replace(" as `fact`", factAlias)
                        .replace("`", this.getAliasQuoteCharacter())
                        .replace("__fACT", element.getKey().toString());
                this.getQueryResponse().getQueryList().add(query);
                this.getQueryResponse().setQueryCount(i++);
                for (Field<BigDecimal> measureField : partialMeasuresCollectionList.get(0)) {
                    this.getSelectQuery().getSelect().remove(measureField);
                }
            }
        }
    }


    private List<Collection<Field<BigDecimal>>> preparePartialMeasuresList(List<AttributeField> partialMeasuresList) {

        List<Collection<Field<BigDecimal>>> measuresCollectionList = new ArrayList<>();

        Collection<Field<BigDecimal>> selectMeasures = new ArrayList<>();
        Collection<Field<BigDecimal>> innerSelectMeasures = new ArrayList<>();

        List<String> quotedPartialMeasuresList = partialMeasuresList.stream().map(measure -> "'" + measure.getColumnName() + "'").collect(Collectors.toList());
        try {
            if(quotedPartialMeasuresList.size() > 0) {
                List<HashMap<String, String>> measureFieldsMetaData = this.getQbMetaDao().getMeasureFieldsMetaData(
                        this.getMetaQuery().getDialect().name(),
                        String.join(",", quotedPartialMeasuresList)
                        );

                String prevDerivedColumn = "";

                for(HashMap<String, String> rs : measureFieldsMetaData) {

                    if(rs.get("is_derived_column").equals("Y")){
                        Field<BigDecimal> selectField = DSL.field("fact." + rs.get("COLUMN_NAME"), BigDecimal.class);
                        if(rs.get("AGGREGATE_FUNCTION") == null || rs.get("AGGREGATE_FUNCTION").isEmpty()){
                            // selectMeasures.add(selectField.as(rs.get("variable_as_column_alias")));
                            innerSelectMeasures.add(selectField.as(rs.get("variable_as_column_alias")));
                        }
                        else {
                            // selectMeasures.add(selectField.sum().as(rs.get("variable_as_column_alias")));
                            innerSelectMeasures.add(selectField.sum().as(rs.get("variable_as_column_alias")));
                        }
                        if( !rs.get("unique_column_name").equals(prevDerivedColumn) ) {
                            Field<BigDecimal> derivedSelectField = DSL.field(rs.get("derived_column_formula"), BigDecimal.class);
                            selectMeasures.add(derivedSelectField.as(rs.get("unique_column_name")));
                        }
                        prevDerivedColumn = rs.get("unique_column_name");
                    }
                    else {
                        Field<BigDecimal> selectField = DSL.field("fact." + rs.get("COLUMN_NAME"), BigDecimal.class);
                        if(rs.get("AGGREGATE_FUNCTION") == null || rs.get("AGGREGATE_FUNCTION").isEmpty()){
                            selectMeasures.add(selectField.as(rs.get("COLUMN_NAME")));
                            innerSelectMeasures.add(selectField.as(rs.get("COLUMN_NAME")));
                        }
                        else {
                            selectMeasures.add(selectField.sum().as(rs.get("COLUMN_NAME")));
                            innerSelectMeasures.add(selectField.sum().as(rs.get("COLUMN_NAME")));
                        }
                    }
                }
            }
        } catch (SQLException e) {
            this.getQueryResponse().setError( this.getQueryResponse().getError() + e.getMessage() + StackTraceUtil.stackTraceToString(e));
        }

        measuresCollectionList.add(selectMeasures);
        measuresCollectionList.add(innerSelectMeasures);

        return measuresCollectionList;
    }

    @Override
    public void prepareFrom(){

        if(!this.isDimensionOnlyQuery()) {
            try {
                List<HashMap<String,String>> factTablesWithAllRequiredDimensions = this.getQbMetaDao().getFactTablesWithAllRequiredDimensions(
                        this.getMetaQuery().getDialect().name(),
                        this.getKeyColumnsList().stream().map(addQuote).collect(Collectors.joining(",")),
                        this.getKeyColumnsList().size());

                if (factTablesWithAllRequiredDimensions.size() > 0) {
                    for( HashMap<String, String> dRs : factTablesWithAllRequiredDimensions) {
                        if (Integer.parseInt(dRs.get("COLUMN_COUNT")) == this.getKeyColumnsList().size()) {

                            String primaryFactTable = dRs.get("FACT_TABLE_NAME");

                            List<AttributeField> measuresClone = this.getMetaQuery().getMeasures().stream().collect(Collectors.toList());

                            if (this.getMetaQuery().getMeasures().size() > 0) {

                                List<HashMap<String,String>> factTablesWithMaximumRequiredMeasures = this.getQbMetaDao().getFactTablesWithMaximumRequiredMeasures(
                                        this.getMetaQuery().getDialect().name(),
                                        this.getMetaQuery().getMeasureColumns(),
                                        this.getKeyColumnsList().stream().map(addQuote).collect(Collectors.joining(",")),
                                        this.getKeyColumnsList().size());

                                int measureCount = 0;
                                for(HashMap<String,String> row : factTablesWithMaximumRequiredMeasures) {
                                    if (Integer.parseInt(row.get("COLUMN_COUNT")) == this.getMetaQuery().getMeasures().size()) {
                                        //The fact table provides all dimensions and measures.
                                        if (row.get("FACT_TABLE_NAME").equals(primaryFactTable)) {
                                            this.getSelectedFactTables().put(primaryFactTable, this.getMetaQuery().getMeasures());
                                        } else {
                                            this.getSelectedFactTables().put(row.get("FACT_TABLE_NAME"), this.getMetaQuery().getMeasures());
                                        }
                                        measuresClone.clear();
                                        break;
                                    } else {
                                        this.setMultiFactQuery(true);
                                        String strMeasures = row.get("COLUMN_NAMES").toLowerCase();

                                        List<AttributeField> measuresList = new ArrayList<>();
                                        for(Iterator<AttributeField> iterator = measuresClone.iterator(); iterator.hasNext();){
                                            AttributeField fld = iterator.next();
                                            if (strMeasures.contains(fld.getColumnName().toLowerCase())) {
                                                measuresList.add(fld);
                                                primaryFactTable = row.get("FACT_TABLE_NAME");
                                                iterator.remove();
                                                measureCount++;
                                            }
                                        }
                                        if (measuresList.size() > 0) {
                                            this.getSelectedFactTables().put(primaryFactTable, measuresList);
                                        }
                                        if (this.getMetaQuery().getMeasures().size() == measureCount ) break;
                                    }
                                }
                                if(measuresClone.size() > 0){
                                    this.getQueryResponse().setWarning(String.format("No fact table found that has measures '%1$s' with dimension keys '%2$s'! The generated query might project no/part of attributes and/or measures requested!", measuresClone.stream().map(measure -> "'" + measure.getColumnName() + "'").collect(Collectors.toList()), this.getKeyColumnsList().stream().map(addQuote).collect(Collectors.joining(",") ) ) );
                                }
                            } else {
                                this.getSelectedFactTables().put(primaryFactTable, measuresClone);
                                break;
                            }
                            break;
                        }
                    }
                } else {
                    this.getQueryResponse().setError("Sorry ... Found 0 Fact Table(s) that can answer all attributes together!");
                }
            } catch (SQLException e) {
                e.printStackTrace();
                this.getQueryResponse().setError(this.getQueryResponse().getError() + e.getMessage() + StackTraceUtil.stackTraceToString(e));
            }
        }else {
            try {

                List<String> columnNames = new ArrayList<>();
                columnNames.add(this.getMetaQuery().getAttributeColumns());
                columnNames.add(this.getMetaQuery().getFilterColumns());

                List<HashMap<String,String>> dimensionTable = this.getQbMetaDao().getDimensionTable(
                        this.getMetaQuery().getDialect().name(),
                        String.join(",", columnNames) );

                if (dimensionTable.size() > 0) {
                    for (HashMap<String, String> dRs : dimensionTable) {
                        String primaryFactTable = dRs.get("TABLE_NAME") + " AS " + dRs.get("TABLE_ALIAS");
                        this.getSelectedFactTables().put(primaryFactTable, new ArrayList<>());
                    }
                } else {
                    this.getQueryResponse().setError("Sorry.. found 0 dimension Table(s) !");
                }
            } catch (SQLException e) {
                e.printStackTrace();
                this.getQueryResponse().setError(this.getQueryResponse().getError() + e.getMessage() + StackTraceUtil.stackTraceToString(e));
            }
        }
    }

    @Override
    public void prepareFilters() {

        try {
            Condition condition;
            condition = null;
            HashMap<String, HashMap<String, String>> filterMetadata = new HashMap<>();

            List<HashMap<String,String>> filterColumnDetails = this.getQbMetaDao().getFilterColumnDetails(
                    this.getMetaQuery().getDialect().name(),
                    this.getMetaQuery().getFilterColumns());

            for( HashMap<String, String> rs : filterColumnDetails){
                filterMetadata.put(rs.get("UNIQUE_COLUMN_NAME"), rs);
            }

            for(Filter filter: this.getMetaQuery().getFilters()){
                Condition partialCondition = processFilters(filter, filterMetadata);
                if(condition == null){
                    condition = (partialCondition != null)? partialCondition : null;
                }
                else {
                    condition = (partialCondition != null)? condition.and(partialCondition) : condition;
                }
            }

            if(condition != null) {
                if(this.isHasDerivedMeasures()) {
                    this.getInnerSelectQuery().addConditions(condition);
                }
                else {
                    this.getSelectQuery().addConditions(condition);
                }
            }
        } catch (Exception ex){
            this.getQueryResponse().setError( this.getQueryResponse().getError() + ex.getMessage() + StackTraceUtil.stackTraceToString(ex));
        }
    }

    @Override
    public void addOrderBy() {
        if( this.getMetaQuery().getOrderBy() != null && this.getMetaQuery().getOrderBy().length != 0){
            for(OrderField orderField: this.getMetaQuery().getOrderBy()){
                SortField<?> orderFld = (orderField.getOrderBy().ordinal() != 0 )? DSL.field(orderField.getColumnName()).desc() : DSL.field(orderField.getColumnName()).asc();
                this.getSelectQuery().addOrderBy(orderFld);
            }
        }
    }

    @Override
    public void addLimitandOffset() {
        this.getSelectQuery().addLimit(DSL.inline(this.getMetaQuery().getOffset()), DSL.inline(this.getMetaQuery().getLimit()));
    }

    @Override
    public QueryResponse getFormattedQuery() {
        this.setEnablePrettyFormatting(true);
        this.initialize();
        return this.getQuery();
    }

    @Override
    public QueryResponse getQuery() {
        this.initialize();
        try {
            if(this.getMetaQuery().getAttributes().size() > 0 || this.getMetaQuery().getFilters().size() > 0) {
                List<String> columnNames = new ArrayList<>();
                columnNames.add(this.getMetaQuery().getAttributeColumns());
                columnNames.add(this.getMetaQuery().getFilterColumns());
                List<HashMap<String, String>> entityIds = this.getQbMetaDao().getEntityIds(this.getMetaQuery().getDialect().name(), String.join(",", columnNames));
                List<HashMap<String, String>> derivedMeasuresCount = this.getQbMetaDao().getDerivedMeasuresCount(this.getMetaQuery().getDialect().name(), String.join(",", this.getMetaQuery().getMeasureColumns()));
                for (HashMap<String,String> row : entityIds) {
                    this.setEntityIds(row.get("ENTITY_IDS"));
                }
                this.setHasDerivedMeasures( derivedMeasuresCount != null && ! derivedMeasuresCount.get(0).get("no_of_derived_measures").equals("0"));
                this.setDimensionOnlyQuery(this.getEntityIds().split(",").length == 1 && this.getMetaQuery().getMeasures().size() == 0);
            }
            if(! this.isDimensionOnlyQuery()) {
                this.prepareDateTimeFilter();
                // if(!this.metaQuery.getDialect().equals(MetaQuery.Dialect.LENS))
                this.prepareJoins();
            }
            this.prepareSelect();
            this.prepareFrom();
            this.prepareFilters();
            this.prepareSelectMeasures();
            this.getQueryResponse().setJoinColumns(this.getMetaQuery().getAttributeColumns().replace("'","").toLowerCase());
        } catch (Exception ex){
            this.getQueryResponse().setError( this.getQueryResponse().getError() + ex.getMessage() + StackTraceUtil.stackTraceToString(ex));
        }
        return this.getQueryResponse();
    }

    private Condition processFilters(Filter filter, HashMap<String, HashMap<String, String>> filterMetaData) throws Exception {

        Condition condition = null;
        if( filter.type == Filter.FilterType.simple){

            SimpleFilter simpleFilter = (SimpleFilter) filter.getObjFilter();
            String key = simpleFilter.getFilterColumn();

            if(filterMetaData.get(key) != null){

                String columnName = filterMetaData.get(key).get("COLUMN_NAME");
                String alias = filterMetaData.get(key).get("ALIAS");
                String type = filterMetaData.get(key).get("TYPE_NAME");

                alias = (alias.length() > 0)? alias + "." : "";

                String filterValueType;
                List<String> stringFilterValues = new ArrayList<>();
                List<Long> numberFilterValues = new ArrayList<>();
                Map<String, List<?>> mixedTypeLists = new HashMap<>();

                Field<String> sfield = DSL.field(alias + columnName, String.class);
                Field<Long> lfield = DSL.field(alias + columnName, Long.class);
                Map<String, Field<?>> mixedTypeField = new HashMap<>();


                Comparator comparator = null;
                if( getSingleValueOperators().contains(simpleFilter.getOperator())){
                    comparator = Comparator.valueOf(simpleFilter.getOperator().name());
                }

                /*
                Do not know the type of the values upfront
                hence using 'filterValueType' to pass dynamically ('strings'/'numbers') to use
                */

                if(!getNumberTypeNames().contains(type)){
                    stringFilterValues = simpleFilter.getFilterValue();
                    filterValueType = "strings";
                }
                else {
                    try{
                        numberFilterValues.addAll(simpleFilter.getFilterValue().stream().map(Long::parseLong).collect(Collectors.toList()));
                        filterValueType = "numbers";
                    }
                    catch (NumberFormatException e){
                        throw new Exception("Invalid filterValue for column type" + type);
                    }
                }
                mixedTypeLists.put("strings", stringFilterValues);
                mixedTypeLists.put("numbers", numberFilterValues);

                mixedTypeField.put("strings", sfield);
                mixedTypeField.put("numbers", lfield);

                if( getMultiValueOperators().contains(simpleFilter.getOperator()) && simpleFilter.getFilterValue().size() > 0 ){
                    switch (simpleFilter.getOperator()) {
                        default:
                        case IN:
                            condition = mixedTypeField.get(filterValueType).in(mixedTypeLists.get(filterValueType));
                            break;
                        case NOT_IN:
                            condition = mixedTypeField.get(filterValueType).notIn(mixedTypeLists.get(filterValueType));
                            break;
                    }
                } else if( getSingleValueOperators().contains(simpleFilter.getOperator()) && simpleFilter.getFilterValue().size() == 1 ){
                    condition = sfield.compare(comparator, mixedTypeLists.get(filterValueType).get(0).toString());
                } else if( getDualValueOperators().contains(simpleFilter.getOperator()) && simpleFilter.getFilterValue().size() == 2 ){
                    condition = sfield.between( DSL.inline(mixedTypeLists.get(filterValueType).get(0).toString()), DSL.inline(mixedTypeLists.get(filterValueType).get(1).toString()));
                } else {
                    throw new Exception("inadequate / too many filter values for operator : " + simpleFilter.getOperator().name());
                }
            }
            else {
                this.getQueryResponse().setWarning(this.getQueryResponse().getWarning() + "| Filter on Column : " + simpleFilter.getFilterColumn() + ", has been ignored! due to non availability of metadata! \n" );
            }
        }
        else {
            ComplexFilter complexFilter = (ComplexFilter) filter.getObjFilter();
            for (Filter filter1 : complexFilter.getFilters()){
                if(condition == null) {
                    condition = processFilters(filter1, filterMetaData);
                }
                else{
                    Condition innerCondition = processFilters(filter1, filterMetaData);
                    if(innerCondition != null) {
                        condition = (complexFilter.getQualifier() == ComplexFilter.Qualifier.AND) ? condition.and(innerCondition) : condition.or(innerCondition);
                    }
                }
            }
        }
        return condition;
    }
}