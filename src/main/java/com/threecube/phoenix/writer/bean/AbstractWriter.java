package com.threecube.phoenix.writer.bean;

import com.threecube.phoenix.writer.util.ConstentsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by Mike Ding on 2015/11/16.
 */
public class AbstractWriter {

    public Table init(){
        Table table = null;
        try{
            Configuration conf = HBaseConfiguration.create();
            conf.addResource(new Path("src/main/resources/hdfs-site.xml"));
            Connection connection = ConnectionFactory.createConnection(conf);
            table =  connection.getTable(TableName.valueOf(ConstentsUtil.tabname));

            Admin admin = connection.getAdmin();

            if(admin.tableExists(TableName.valueOf(ConstentsUtil.tabname))){
                admin.disableTable(TableName.valueOf(ConstentsUtil.tabname));
                admin.deleteTable(TableName.valueOf(ConstentsUtil.tabname));
            }

            HTableDescriptor tableDescriptor = new HTableDescriptor(Bytes.toBytes(ConstentsUtil.tabname));
            tableDescriptor.addFamily(new HColumnDescriptor(ConstentsUtil.HBASE_GLOBAL_CF));
            tableDescriptor.addFamily(new HColumnDescriptor(ConstentsUtil.HBASE_EVENT_CF));
            tableDescriptor.addFamily(new HColumnDescriptor(ConstentsUtil.HBASE_EXTENSION_CF));

            admin.createTable(tableDescriptor);
        }catch(IOException e){
            e.printStackTrace();
        }

        return table;
    }
}
