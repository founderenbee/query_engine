package com.inmobi.corp.clarity.utils;


import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public  class ResultSetUtil {

    public static List<HashMap<String, String>> resultSetToArrayList(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        ArrayList<HashMap<String, String>> list = new ArrayList<>(50);
        while (rs.next()){
            HashMap<String, String> row = new HashMap<>(columns);
            for(int i=1; i<=columns; ++i){
                row.put(md.getColumnName(i),rs.getString(i));
            }
            list.add(row);
        }
        return list;
    }

}
