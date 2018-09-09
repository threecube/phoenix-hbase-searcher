package com.threecube.phoenix.service;

import com.threecube.phoenix.result.LogSearchResult;

import java.util.List;

/**
 * Created by Mike Ding on 2015/11/24.
 */
public interface LogSearchService {

    public LogSearchResult searchBySql(String sql);

    public List<String> getColumns();

    public String getById(String id);
}
