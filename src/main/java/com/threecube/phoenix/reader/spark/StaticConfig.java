package com.threecube.phoenix.reader.spark;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by Mike Ding on 2015/11/25.
 */
public class StaticConfig {

    public static JavaSparkContext javaSc = null;

    public static SQLContext sqlContext = null;

    public static String sparkMaster = "local[1]";

    public static void init(){
        SparkConf sparkConf = new SparkConf().setAppName("fucker").setMaster(sparkMaster);

        javaSc = new JavaSparkContext(sparkConf);
        sqlContext = new SQLContext(javaSc);
    }

    public static void close(){
        sqlContext.clearCache();
        javaSc.stop();
    }
}
