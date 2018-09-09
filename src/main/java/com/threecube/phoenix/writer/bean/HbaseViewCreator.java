package com.threecube.phoenix.writer.bean;

import com.threecube.phoenix.writer.util.ConstentsUtil;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Mike Ding on 2015/11/23.
 */
public class HbaseViewCreator {

    private Connection connection = null;
    private String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
    private String hbaseUrl = "jdbc:phoenix:slave2.hadoop.pt2:2181";

    private Map globalKeyMap = new HashMap<String, Integer>();
    private Map eventKeyMap = new HashMap<String, Integer>();
    private Map extensionKeyMap = new HashMap<String, Integer>();

    private boolean isCreateView;

  public HbaseViewCreator(boolean isCreateView){
      this.isCreateView = isCreateView;
        try {
            Class.forName(driver);

            connection = DriverManager.getConnection(hbaseUrl);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createView(){
        if (!isCreateView){
            return ;
        }

        String viewSql = genViewSql();
        System.out.println("start to create view in hbase [" + viewSql + "].");
        if(viewSql == null || viewSql.length() <=0 ){
            return;
        }
        Statement stmt = null;

        try {
            long startTime = System.currentTimeMillis();
            stmt = connection.createStatement();
            stmt.executeUpdate("drop view " + ConstentsUtil.tabname);
            stmt.executeUpdate(viewSql);
            connection.commit();
            System.out.println("success to create view in hbase [" + viewSql + "].");
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(connection != null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private String genViewSql(){
        String viewSql = "create view " + ConstentsUtil.tabname + "( UUID VARCHAR PRIMARY KEY,";
        String varchar = "VARCHAR";

        //for global key
        Set globalSet = globalKeyMap.keySet();
        for(Object key : globalSet){
            if(key.equals("UUID")){
                continue;
            }
            String columnNm = ConstentsUtil.HBASE_GLOBAL_CF + "." + (String)key + " " + varchar + ",";
            viewSql += columnNm;
        }

        //for event key
        Set eventSet = eventKeyMap.keySet();
        for(Object key : eventSet){
            String columnNm = ConstentsUtil.HBASE_EVENT_CF + "." + (String)key + " " + varchar + ",";
            viewSql += columnNm;
        }

        //for extension key
        Set extSet = extensionKeyMap.keySet();
        for(Object key : extSet){
            String columnNm = ConstentsUtil.HBASE_EXTENSION_CF + "." + (String)key + " " + varchar + ",";
            viewSql += columnNm;
        }
        viewSql = viewSql.substring(0, viewSql.length()-1);
        viewSql += ")";
        System.out.println(viewSql);
        return viewSql;
    }

    public void setGlobalKey(Object... keys){
        if(keys == null || keys.length <= 0){
            return ;
        }
        for(Object key : keys){
            globalKeyMap.put((String)key, 1);
        }
    }

    public void setEventKey(Object... keys){
        if(keys == null || keys.length <= 0){
            return ;
        }
        for(Object key : keys){
            eventKeyMap.put((String)key, 1);
        }
    }

    public void setExtKey(Object... keys){
        if(keys == null || keys.length <= 0){
            return ;
        }
        for(Object key : keys){
            extensionKeyMap.put((String)key, 1);
        }
    }
}
