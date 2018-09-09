package com.threecube.phoenix.writer.bean;

import com.threecube.phoenix.writer.model.HbaseRowModel;
import com.threecube.phoenix.writer.util.ConstentsUtil;
import com.threecube.phoenix.writer.result.WriterResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by Mike Ding on 2015/11/16.
 */
public class JsonFileWriter {

    private Table table = null;

    public JsonFileWriter(boolean isReCreateTable){
        try{
            Configuration conf = HBaseConfiguration.create();
            conf.addResource(new Path("src/main/resources/hdfs-site.xml"));
            Connection connection = ConnectionFactory.createConnection(conf);
            table =  connection.getTable(TableName.valueOf(ConstentsUtil.tabname));

            if(!isReCreateTable){
                return;
            }

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
    }

    public WriterResult write(List<HbaseRowModel> buffer){

        WriterResult result = new WriterResult();
        long num = 0;
        List<Put> putList = new ArrayList<Put>();
        try{
            for(HbaseRowModel model : buffer){
                Put put = new Put(Bytes.toBytes(model.getRowKey()));
                generatePut(put, ConstentsUtil.HBASE_GLOBAL_CF, model.getGlobalCf());
                generatePut(put, ConstentsUtil.HBASE_EVENT_CF, model.getEventCf());
                generatePut(put, ConstentsUtil.HBASE_EXTENSION_CF, model.getExtensionCf());
                putList.add(put);
                num ++;
            }
            table.put(putList);
            //putList.clear();
            System.out.println("success to write data[number:" + num + "] into habse");
        }catch(IOException e){
            e.printStackTrace();
            result.setIsSuccess(false);
            result.setErrCode(e.hashCode());
            result.setErrMsg(e.getMessage());
            return result;
        }finally {
            if(table != null){
                try{
                    table.close();
                }catch(IOException e){
                    e.printStackTrace();
                }

            }
        }

        result.setIsSuccess(true);
        result.setDataNum(num);
        return result;
    }

    private void generatePut(Put put, String clusterName, Map dataMap){
        Iterator it = dataMap.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry entry = (Map.Entry)it.next();
            String column = (String)entry.getKey();
            String value = (String)entry.getValue();
            put.addColumn(Bytes.toBytes(clusterName), Bytes.toBytes(column), Bytes.toBytes(value));
        }
    }


}
