/**
 * 
 */
package com.threecube.phoenix.service.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import com.threecube.phoenix.model.LogQueryParams;
import com.threecube.phoenix.result.LogSearchResult;
import com.threecube.phoenix.service.PhoenixQueryService;

/**
 * @author wenbin_dwb
 *
 */
@Service
public class PhoenixQueryServiceImpl extends AbstractPhoenixService implements PhoenixQueryService {
	
	private static final String QUERY_SQL = "select %s from %s";
	
	private static final String QUERY_SQL_WITH_CONDITION = "select %s from %s where %s";
    
	/* (non-Javadoc)
	 * @see com.threecube.ph.service.PhoenixHbaseQuery#query(com.threecube.ph.model.LogQueryParams)
	 */
	public LogSearchResult query(LogQueryParams queryParams) throws Exception {
		
		LogSearchResult result = new LogSearchResult();
		
		if(queryParams == null) {
			throw new Exception("参数不能为null");
		}
		
		if(StringUtils.isBlank(queryParams.getTableName()) || 
				CollectionUtils.isEmpty(queryParams.getQueryColumns())) {
			throw new Exception("参数错误");
		}
		
		String querySql = generateSql(queryParams);
		
		List<String> columns = new ArrayList<String>();
        List<List<String>> values = new ArrayList<List<String>>();

        boolean done = false;
        long num = 0;
        PreparedStatement statement = connection.prepareStatement(querySql);
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
        
		return result;
	}
	
	/**
	 * 生成sql语句
	 * 
	 * @param queryParams
	 * @return
	 */
	private String generateSql(LogQueryParams queryParams) {
		
		String queryClomunStr = null;
		for(String queryColumn : queryParams.getQueryColumns()) {
			if(queryClomunStr == null) {
				queryClomunStr = queryColumn;
			} else {
				queryClomunStr = queryClomunStr + "," + queryColumn;
			}
		}
		
		String conditions = null;
		if(queryParams.getQueryConditons() != null && queryParams.getQueryConditons().size() > 0) {
			Map<String, Object> conditionMap = queryParams.getQueryConditons();
			for(Entry<String, Object> entry : conditionMap.entrySet()) {
				
				if(conditions == null) {
					if(entry.getValue() instanceof String) {
						conditions = String.format("%s='%s'", entry.getKey(), entry.getValue());
					} else {
						conditions = String.format("%s=%s", entry.getKey(), entry.getValue());
					}
				} else {
					if(entry.getValue() instanceof String) {
						conditions = String.format("%s, %s='%s'", conditions, entry.getKey(), entry.getValue());
					} else {
						conditions = String.format("%s, %s=%s", conditions, entry.getKey(), entry.getValue());
					}
				}
			}
		}
		
		if(StringUtils.isBlank(conditions)) {
			return String.format(QUERY_SQL, queryClomunStr, queryParams.getTableName());
		} else {
			return String.format(QUERY_SQL_WITH_CONDITION, queryClomunStr, queryParams.getTableName(), conditions);
		}
	}
	
}
