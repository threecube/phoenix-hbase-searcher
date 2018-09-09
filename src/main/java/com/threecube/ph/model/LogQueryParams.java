/**
 * 
 */
package com.threecube.ph.model;

import java.util.List;
import java.util.Map;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

/**
 * log查询参数
 * 
 * @author wenbin_dwb
 *
 */
@Data
public class LogQueryParams {
	
	/**
	 * 表名
	 */
	private String tableName;
	
	/**
	 * 查询列
	 */
	private List<String> queryColumns;
	
	/**
	 * 查询条件
	 */
	private Map<String, Object> queryConditons;
}
