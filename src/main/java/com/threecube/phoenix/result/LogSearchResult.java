package com.threecube.phoenix.result;

import java.util.List;

/**
 * Created by Mike Ding on 2015/11/24.
 */
public class LogSearchResult extends BaseResult{

    private long logNum;

    private List<String> columns;

    private List<List<String>> values;

    public long getLogNum() {
        return logNum;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<List<String>> getValues() {
        return values;
    }

    public void setLogNum(long logNum) {
        this.logNum = logNum;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public void setValues(List<List<String>> values) {
        this.values = values;
    }
}
