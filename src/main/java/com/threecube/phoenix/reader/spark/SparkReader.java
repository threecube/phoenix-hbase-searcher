package com.threecube.phoenix.reader.spark;

import com.threecube.phoenix.result.LogSearchResult;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import java.util.*;

/**
 * Created by Mike Ding on 2015/11/18.
 */
public class SparkReader {

    public SparkReader(){
        new SparkDfLoader();
    }

    public void query(String sql, LogSearchResult result) {
        try {
            DataFrame dfData = StaticConfig.sqlContext.sql(sql);
            if(dfData == null){
                result.setSuccess(false);
                result.setErrMsg("load dataframe is null");
                return;
            }

            result.setLogNum(dfData.count());
            result.setColumns(Arrays.asList(dfData.columns()));

            List<List<String>> values = new ArrayList<List<String>>();
            Row[] rowList= dfData.collect();
            for(Row row : rowList){
                values.add((List<String>)row.get(0));
            }
            result.setValues(values);
            return;

        }catch(Exception e){
            result.setSuccess(false);
            result.setErrMsg(e.getMessage());
            return;
        }
    }

}
