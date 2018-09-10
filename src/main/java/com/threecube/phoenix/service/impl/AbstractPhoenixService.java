/**
 * 
 */
package com.threecube.phoenix.service.impl;

import java.sql.Connection;
import java.sql.DriverManager;

import lombok.extern.slf4j.Slf4j;

/**
 * @author wenbin_dwb
 *
 */
@Slf4j
public class AbstractPhoenixService {
	
	private static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    private static final String HBASE_URL = "jdbc:phoenix:slave2.hadoop.pt2:2181";
    
    protected static Connection connection = null;
    
    public AbstractPhoenixService() {
    	
    		createConnection();
    		
    		Runtime.getRuntime().addShutdownHook(new Thread() {
    			
    			@Override
    			public void run() {
    				if(AbstractPhoenixService.connection != null) {
    					log.info("关闭habse连接");
    					
    					try {
    						AbstractPhoenixService.connection.close();
    					} catch(Exception e) {
    						log.error("关闭hbase连接失败", e);
    					}
    				}
    			}
    		});
    }
    
    private void createConnection() {
		
		if(this.connection == null) {
			try {
				Class.forName(PHOENIX_DRIVER);
				this.connection = DriverManager.getConnection(HBASE_URL);
				if(this.connection == null) {
					throw new Exception("创建habse连接为null");
				}
			} catch(Exception e) {
				log.error("创建habse连接失败", e);
				System.exit(-1);
			}
		}
	}
}
