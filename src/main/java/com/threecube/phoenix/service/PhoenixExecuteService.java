/**
 * 
 */
package com.threecube.phoenix.service;

import java.util.List;

/**
 * @author wenbin_dwb
 *
 */
public interface PhoenixExecuteService {
	
	public boolean createIndex(String tableName, String indexName, List<String> columns) throws Exception;
	
	public boolean dropIndex(String tableName, String indexName) throws Exception;
	
	public boolean createView(String tableName, String viewName, List<String> columns) throws Exception;
	
	public boolean dropView(String tableName, String viewName) throws Exception;
}
