package com.threecube.phoenix.service.impl;

import com.threecube.phoenix.reader.phoenix.PhoenixReader;
import com.threecube.phoenix.result.LogSearchResult;
import com.threecube.phoenix.service.LogSearchService;

import java.util.List;

/**
 * log search by phoenix based on hbase
 *
 * Created by Mike Ding on 2015/11/24.
 */
public class PhoenixSearchImpl implements LogSearchService{

    private PhoenixReader reader = new PhoenixReader();

    public LogSearchResult searchBySql(String sql) {
        LogSearchResult result = new LogSearchResult();
        if(sql == null || sql.length() <= 0){
            result.setSuccess(false);
            result.setErrMsg("input parameter is null");
            return result;
        }

        reader.query(sql, result);
        return result;
    }

    public List<String> getColumns() {
        return null;
    }

    public String getById(String id) {
        return null;
    }
}
