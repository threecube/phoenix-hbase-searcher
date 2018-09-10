/**
 * 
 */
package com.threecube.phoenix.service.impl;

import java.sql.PreparedStatement;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.threecube.phoenix.service.PhoenixExecuteService;

import lombok.extern.slf4j.Slf4j;

/**
 * @author wenbin_dwb
 *
 */
@Slf4j
public class PhoenixExecuteServiceImpl extends AbstractPhoenixService implements PhoenixExecuteService {
	
	private static final String CREATE_INDEX_SQL = "create index %s on %s(%s)";
	
	private static final String DROP_INDEX_SQL = "drop index %s on %s";
	
	/* (non-Javadoc)
	 * @see com.threecube.ph.service.PhoenixExecuteService#createIndex(java.lang.String, java.lang.String, java.util.List)
	 */
	@Override
	public boolean createIndex(String tableName, String indexName, List<String> columns) throws Exception {
		
		if(StringUtils.isBlank(tableName) || StringUtils.isBlank(indexName) ||
				CollectionUtils.isEmpty(columns)) {
			log.error("入参不能为空");
			throw new Exception("入参不能为空");
		}
		
		String columStr = null;
		for(String column : columns) {
			if(columStr == null) {
				columStr = column;
			} else {
				columStr = columStr + "," + column;
			}
		}
		
		String sql = String.format(CREATE_INDEX_SQL, indexName, tableName, columStr);
		
		log.info("创建索引，{}", sql);
		
        PreparedStatement statement = connection.prepareStatement(sql);
        return statement.execute();
	}

	/* (non-Javadoc)
	 * @see com.threecube.ph.service.PhoenixExecuteService#dropIndex(java.lang.String, java.lang.String)
	 */
	@Override
	public boolean dropIndex(String tableName, String indexName) throws Exception {
		
		if(StringUtils.isBlank(tableName) || StringUtils.isBlank(indexName)) {
			log.error("入参不能为空");
			throw new Exception("入参不能为空");
		}
		
		String sql = String.format(DROP_INDEX_SQL, indexName, tableName);
		
		log.info("删除索引，{}", sql);
		
        PreparedStatement statement = connection.prepareStatement(sql);
        return statement.execute();
	}

	@Override
	public boolean createView(String tableName, String viewName, List<String> columns) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean dropView(String tableName, String viewName) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

}
