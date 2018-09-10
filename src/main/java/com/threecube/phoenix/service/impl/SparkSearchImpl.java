package com.threecube.phoenix.service.impl;

import com.threecube.phoenix.reader.spark.SparkReader;
import com.threecube.phoenix.reader.spark.StaticConfig;
import com.threecube.phoenix.result.LogSearchResult;
import com.threecube.phoenix.service.LogSearchService;

import java.util.List;

/**
 *  log search by spark based on hbase
 *
 * Created by Mike Ding on 2015/11/24.
 */
public class SparkSearchImpl implements LogSearchService{

    SparkReader sparkReader = new SparkReader();

    public LogSearchResult searchBySql(String sql) {
        LogSearchResult result = new LogSearchResult();

        if(sql == null || sql.length() <= 0){
            result.setSuccess(false);
            result.setErrMsg("input parameter is null");
            return result;
        }

        sparkReader.query(sql, result);
        return result;
    }

    public List<String> getColumns() {
        return null;
    }

    public String getById(String id) {
        return null;
    }

    public void init(){
        StaticConfig.init();
    }
}
