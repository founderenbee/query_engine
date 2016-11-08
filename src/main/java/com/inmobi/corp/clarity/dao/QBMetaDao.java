package com.inmobi.corp.clarity.dao;

import com.inmobi.corp.clarity.utils.ResultSetUtil;
import lombok.Getter;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

@Repository
public class QBMetaDao {

    private static final Logger log = LoggerFactory.getLogger(QBMetaDao.class.getName());

    @Autowired()
    @Getter
    private DSLContext metaDSLContext;

    /**
     * @return  columns entity_name AS TYPE, entity_attribute_name AS NAME, entity_attribute_caption AS CAPTION
     */
    public String getAvailableFieldsList(String dataSourceName ){
        String sqlQuery = "SELECT DISTINCT entity_name AS TYPE, entity_attribute_name AS NAME, entity_attribute_caption AS CAPTION\n" +
                    "FROM  rbac_entity_attribute AS a\n" +
                    "INNER JOIN rbac_entity AS b ON a.entity_id = b.entity_id\n" +
                    "INNER JOIN qb_dimension_column_map AS c ON c.entity_attribute_id = a.entity_attribute_id\n" +
                    "INNER JOIN qb_join_chain AS d ON d.table_id = c.table_id\n" +
                    "INNER JOIN m_datasource AS e ON e.id = d.datasource_id\n" +
                    "WHERE e.name LIKE '%1$s'\n" +
                    "UNION ALL\n" +
                    "SELECT DISTINCT 'MEASURES' AS TYPE, unique_column_name AS NAME, UPPER(unique_column_name) AS CAPTION \n" +
                    "FROM qb_fact_column_map f\n" +
                    "INNER JOIN m_datasource AS g ON g.id = f.datasource_id\n" +
                    "WHERE IS_MEASURE = 'Y' AND g.name LIKE '%1$s'\n" +
                    "ORDER BY 1, 2;";
        if (dataSourceName.isEmpty() || dataSourceName.equals("")){
            dataSourceName = "%";
        }
        return this.getMetaDSLContext().fetch( String.format(sqlQuery, dataSourceName)).formatJSON();
    }

    /**
     * @param dataSourceName TBD
     * @param columnNamesofAttributesAndFilters TBD
     * @return TBD
     * @throws SQLException
     */
    public List<HashMap<String,String>> getEntityIds(String dataSourceName, String columnNamesofAttributesAndFilters)  throws SQLException{
        String entityIds =  "SELECT \tGROUP_CONCAT(DISTINCT  a.table_id) ENTITY_IDS \n" +
                "FROM qb_join_chain AS a \n" +
                "INNER JOIN qb_dimension_column_map AS b ON a.table_id = b.table_id \n" +
                "INNER JOIN rbac_entity_attribute AS c ON c.entity_attribute_id = b.entity_attribute_id \n" +
                "INNER JOIN m_datasource AS d ON d.id = a.datasource_id \n" +
                "WHERE d.name = '%1$s' AND c.entity_attribute_name IN ( %2$s ) ;";
        return this.getMetaData(String.format(entityIds, dataSourceName, columnNamesofAttributesAndFilters));
    }

    /**
     * @param dataSourceName TBD
     * @param measureNames TBD
     * @return TBD
     * @throws SQLException
     */
    public List<HashMap<String,String>> getDerivedMeasuresCount(String dataSourceName, String measureNames)  throws SQLException{
        String derivedMeasuresCount =  "SELECT COUNT(DISTINCT unique_column_name) AS no_of_derived_measures\n" +
                                        "FROM qb_fact_column_map AS b \n" +
                                        "INNER JOIN m_datasource AS d ON d.id = b.datasource_id\n" +
                                        "WHERE d.name = '%1$s' AND b.unique_column_name IN (%2$s) AND IS_MEASURE = 'Y' AND is_derived_column = 'Y';";
        return this.getMetaData(String.format(derivedMeasuresCount, dataSourceName, measureNames));
    }

    /**
     * @param strEntityIds TBD
     * @return TBD
     * @throws SQLException
     */
    public List<HashMap<String,String>> getJoinChainMetadata(String strEntityIds)  throws SQLException{
        String joinChainMetadata =  "SELECT \ta.table_id AS ENTITY_ID, \n" +
                                    "\t\ta.table_name AS ENTITY_NAME, \n" +
                                    "\t\ta.table_alias AS ALIAS, \n" +
                                    "\t\ta.key_column AS KEY_COLUMN, \n" +
                                    "\t\ta.join_key_column AS JOIN_KEY_COLUMN, \n" +
                                    "\t\ta.parent_table_id AS PARENT_ENTITY_ID, \n" +
                                    "\t\tb.table_name AS P_ENTITY_NAME, \n" +
                                    "\t\tb.table_alias AS P_ALIAS, \n" +
                                    "\t\tb.key_column AS P_KEY_COLUMN, \n" +
                                    "\t\tb.join_key_column AS P_JOIN_KEY_COLUMN, \n" +
                                    "\t\tb.parent_table_id AS P_PARENT_ENTITY_ID \n" +
                                    "FROM qb_join_chain AS a\n" +
                                    "LEFT JOIN qb_join_chain AS b ON a.parent_table_id = b.table_id\n" +
                                    "WHERE a.table_id IN (%1$s) ;";
        return this.getMetaData(String.format(joinChainMetadata, strEntityIds));
    }

    /**
     * @param dataSourceName TBD
     * @param attributeNames TBD
     * @return TBD
     * @throws SQLException
     */
    public List<HashMap<String,String>> getSelectFieldsMetadata(String dataSourceName, String attributeNames)  throws SQLException{

        String selectFieldsMetadataQuery = "SELECT \ta.table_id AS ENTITY_ID,\n" +
                "\t\ta.table_name AS ENTITY_NAME,\n" +
                "\t\ta.table_alias AS ALIAS,\n" +
                "\t\tb.column_name AS COLUMN_NAME,\n" +
                "\t\tc.entity_attribute_name AS UNIQUE_COLUMN_NAME\n" +
                "FROM qb_join_chain AS a \n" +
                "INNER JOIN qb_dimension_column_map AS b ON a.table_id = b.table_id \n" +
                "INNER JOIN rbac_entity_attribute AS c ON c.entity_attribute_id = b.entity_attribute_id \n" +
                "INNER JOIN m_datasource AS d ON d.id = a.datasource_id \n" +
                "WHERE d.name = '%1$s' AND c.entity_attribute_name IN ( %2$s ) ;";

        return this.getMetaData(String.format(selectFieldsMetadataQuery, dataSourceName, attributeNames));
    }

    /**
     * @param dataSourceName TBD
     * @param measureNames TBD
     * @return TBD
     * @throws SQLException
     */
    public List<HashMap<String,String>> getMeasureFieldsMetaData(String dataSourceName, String measureNames)  throws SQLException{
        String measureFieldsMetaDataSql =  "SELECT DISTINCT b.COLUMN_NAME,\n" +
                                            "               b.IS_MEASURE,\n" +
                                            "               b.AGGREGATE_FUNCTION,\n" +
                                            "               b.is_derived_column, \n" +
                                            "               b.unique_column_name,\n" +
                                            "               b.variable_as_column_alias,\n" +
                                            "               b.derived_column_formula,\n" +
                                            "               b.data_type_name\n" +
                                            "FROM qb_fact_column_map AS b\n" +
                                            "INNER JOIN m_datasource AS d ON d.id = b.datasource_id\n" +
                                            "WHERE d.name = '%1$s' AND b.unique_column_name IN (%2$s) AND b.IS_MEASURE = 'Y'\n" +
                                            "ORDER BY b.unique_column_name, b.variable_as_column_alias;";
        return this.getMetaData(String.format(measureFieldsMetaDataSql, dataSourceName, measureNames ));
    }

    /**
     * @param datasourceName TBD
     * @param keyColumnsList TBD
     * @param keyColumnsCount TBD
     * @return TBD
     * @throws SQLException
     */
    public  List<HashMap<String, String>> getFactTablesWithAllRequiredDimensions(String datasourceName, String keyColumnsList, int keyColumnsCount)  throws SQLException {
        String factTablesWithAllRequiredDimensions =   "SELECT \ta.fact_table_name AS FACT_TABLE_NAME, \n" +
                "\t\tCOUNT(*) AS COLUMN_COUNT\n" +
                "FROM qb_fact_rank AS a\n" +
                "INNER JOIN qb_fact_column_map AS b ON a.fact_table_id = b.fact_table_id\n" +
                "INNER JOIN m_datasource AS c ON c.id = b.datasource_id \n" +
                "WHERE c.name = '%1$s' AND  b.column_name IN ( %2$s )\n" +
                "GROUP BY a.fact_table_name, a.fact_rank " +
                " HAVING COUNT(*) = %3$s \n" +
                "ORDER BY a.fact_rank";

        return this.getMetaData(String.format(factTablesWithAllRequiredDimensions, datasourceName, keyColumnsList, keyColumnsCount));
    }

    /**
     * @param datasourceName TBD
     * @param measureColumns TBD
     * @param keyColumnsList TBD
     * @param keyColumnsCount TBD
     * @return TBD
     * @throws SQLException
     */
    public  List<HashMap<String, String>> getFactTablesWithMaximumRequiredMeasures(String datasourceName, String measureColumns, String keyColumnsList, int keyColumnsCount)  throws SQLException {

        String factTablesWithMaximumRequiredMeasures =     "SELECT \ta.fact_table_name AS FACT_TABLE_NAME, \n" +
                "\t\tGROUP_CONCAT(DISTINCT b.unique_column_name) AS COLUMN_NAMES, \n" +
                "\t\ta.fact_rank AS RANK,\n" +
                "\t\tCOUNT(DISTINCT b.unique_column_name) AS COLUMN_COUNT\n" +
                "FROM qb_fact_rank AS a\n" +
                "INNER JOIN qb_fact_column_map AS b ON a.fact_table_id = b.fact_table_id\n" +
                "INNER JOIN m_datasource AS c ON c.id = b.datasource_id \n" +
                "WHERE c.name = '%1$s' \n" +
                "AND b.unique_column_name IN ( %2$s )\n" +
                "AND a.fact_table_id IN ( \tSELECT \ta.fact_table_id FROM qb_fact_rank AS a\n" +
                "\t\t\t\t\t\t\tINNER JOIN qb_fact_column_map AS b ON a.fact_table_id = b.fact_table_id\n" +
                "\t\t\t\t\t\t\tINNER JOIN m_datasource AS c ON c.id = b.datasource_id\n" +
                "\t\t\t\t\t\t\tWHERE c.name = '%1$s' AND  b.column_name IN ( %3$s )\n" +
                "\t\t\t\t\t\t\tGROUP BY a.fact_table_name, a.fact_rank\n" +
                "\t\t\t\t\t\t\tHAVING COUNT(*) = %4$s  ) \n" +
                "GROUP BY a.fact_table_name, a.fact_rank \n" +
                "ORDER BY 4 DESC, a.fact_rank, b.column_name;";

        return this.getMetaData(String.format(factTablesWithMaximumRequiredMeasures, datasourceName, measureColumns, keyColumnsList, keyColumnsCount));
    }

    /**
     * @param datasourceName TBD
     * @param attributeColumns TBD
     * @return TBD
     * @throws SQLException
     */
    public List<HashMap<String, String>> getDimensionTable(String datasourceName, String attributeColumns)  throws SQLException{
        String dimensionTable = "SELECT a.table_id AS TABLE_ID, a.table_name AS TABLE_NAME, a.table_alias AS TABLE_ALIAS \n" +
                                "FROM qb_join_chain AS a\n" +
                                "INNER JOIN qb_dimension_column_map AS b ON a.table_id = b.table_id\n" +
                                "INNER JOIN rbac_entity_attribute AS c ON c.entity_attribute_id = b.entity_attribute_id\n" +
                                "INNER JOIN m_datasource AS d ON d.id = a.datasource_id\n" +
                                "WHERE d.name = '%1$s' AND c.entity_attribute_name IN ( %2$s ) ;";

        return this.getMetaData(String.format(dimensionTable, datasourceName, attributeColumns));
    }

    /**
     * @param datasourceName TBD
     * @param filterColumns TBD
     * @return TBD
     * @throws SQLException
     */
    public List<HashMap<String, String>> getFilterColumnDetails(String datasourceName, String filterColumns) throws SQLException{
        String filterColumnDetails =    "SELECT \ta.table_id AS ENTITY_ID,\n" +
                                        "\t\ta.table_name AS ENTITY_NAME,\n" +
                                        "\t\ta.table_alias AS ALIAS,\n" +
                                        "\t\tb.column_name AS COLUMN_NAME,\n" +
                                        "\t\tc.entity_attribute_name AS UNIQUE_COLUMN_NAME,\n" +
                                        "\t\tb.column_data_type AS TYPE_NAME\n" +
                                        "FROM qb_join_chain AS a \n" +
                                        "INNER JOIN qb_dimension_column_map AS b ON a.table_id = b.table_id \n" +
                                        "INNER JOIN rbac_entity_attribute AS c ON c.entity_attribute_id = b.entity_attribute_id \n" +
                                        "INNER JOIN m_datasource AS d ON d.id = a.datasource_id \n" +
                                        "WHERE d.name = '%1$s' AND c.entity_attribute_name IN ( %2$s ) ;";

        return this.getMetaData(String.format(filterColumnDetails, datasourceName, filterColumns));
    }

    /**
     * @param datasourceName TBD
     * @param filterColumns TBD
     * @return TBD
     * @throws SQLException
     */
    public List<HashMap<String, String>> getMeasureFilterColumnDetails(String datasourceName, String filterColumns) throws SQLException{
        String filterColumnDetails =    "SELECT \ta.table_id AS ENTITY_ID,\n" +
                "\t\ta.table_name AS ENTITY_NAME,\n" +
                "\t\ta.table_alias AS ALIAS,\n" +
                "\t\tb.column_name AS COLUMN_NAME,\n" +
                "\t\tc.entity_attribute_name AS UNIQUE_COLUMN_NAME,\n" +
                "\t\tb.column_data_type AS TYPE_NAME\n" +
                "FROM qb_join_chain AS a \n" +
                "INNER JOIN qb_dimension_column_map AS b ON a.table_id = b.table_id \n" +
                "INNER JOIN rbac_entity_attribute AS c ON c.entity_attribute_id = b.entity_attribute_id \n" +
                "INNER JOIN m_datasource AS d ON d.id = a.datasource_id \n" +
                "WHERE d.name = '%1$s' AND c.entity_attribute_name IN ( %2$s ) ;";

        return this.getMetaData(String.format(filterColumnDetails, datasourceName, filterColumns));
    }


    /**
     * @param sqlStatement TBD
     * @return TBD
     * @throws SQLException
     */
    private List<HashMap<String,String>> getMetaData(String sqlStatement) throws SQLException {
        List<HashMap<String,String>> rows = null;
        ResultSet rs = null;
        try {
            rs = this.getMetaDSLContext().fetch(sqlStatement).intoResultSet();
            rows =  ResultSetUtil.resultSetToArrayList(rs);
        }
        catch (SQLException sqlException){
            throw sqlException;
        }
        catch (Exception ex){
            log.error("Error : ", ex);
        }
        finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
        }
        return rows;
    }
}