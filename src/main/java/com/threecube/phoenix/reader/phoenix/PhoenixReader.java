package com.threecube.phoenix.reader.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

import com.threecube.phoenix.result.LogSearchResult;

/**
 * Created by Mike Ding on 2015/11/19.
 */
public class PhoenixReader {

    private static Connection connection = null;

    private static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    private static final  String HBASE_URL = "jdbc:phoenix:slave2.hadoop.pt2:2181";

    public PhoenixReader(){

        try {
            Class.forName(PHOENIX_DRIVER);
            connection = DriverManager.getConnection(HBASE_URL);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void query(String sql, LogSearchResult result){
        List<String> columns = new ArrayList<String>();
        List<List<String>> values = new ArrayList<List<String>>();

        boolean done = false;
        long num = 0;

        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery();

            while(resultSet.next()){
                List<String> columnVl = new ArrayList<String>();
                num ++;
                for( int i = 1 ; i <= resultSet.getMetaData().getColumnCount(); i++){
                    if(!done){
                        columns.add(resultSet.getMetaData().getColumnName(i));
                    }
                    columnVl.add(Bytes.toString(resultSet.getBytes(i)));
                }
                values.add(columnVl);
                done = true;
            }
            result.setSuccess(true);
            result.setLogNum(num);
            result.setColumns(columns);
            result.setValues(values);
        } catch (SQLException e) {
            result.setSuccess(false);
            result.setErrMsg(e.getMessage());
        }
    }


    public boolean execute(String sql){
        try {
            System.out.println("Executing [" + sql + "].");
            PreparedStatement statement = connection.prepareStatement(sql);
            return statement.execute();

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }

    }

    public void createView(String sql){
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
