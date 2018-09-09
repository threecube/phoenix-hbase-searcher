package com.threecube.phoenix.reader.spark;

import com.esotericsoftware.kryo.Kryo;
import com.threecube.phoenix.writer.util.ConstentsUtil;
import com.threecube.phoenix.writer.util.HbaseKeyUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.sql.types.*;
import scala.Tuple2;

import java.util.*;

/**
 * Created by Mike Ding on 2015/11/18.
 */
public class SparkDfLoader implements KryoRegistrator{

    private static Configuration config = null;

    private static DataFrame dataframe = null;

    private static String dfTableName = "df_detector_info";

    public SparkDfLoader(){
        try{
            config = HBaseConfiguration.create();
            config.addResource("src/main/resources/hdfs-site.xml");
            config.set(TableInputFormat.INPUT_TABLE, ConstentsUtil.tabname);
            //config.set(TableInputFormat.SCAN_COLUMN_FAMILY, ConstentsUtil.HBASE_GLOBAL_CF);
            //config.set(TableInputFormat.SCAN_COLUMN_FAMILY, ConstentsUtil.HBASE_EVENT_CF);
            //config.set(TableInputFormat.SCAN_COLUMN_FAMILY, ConstentsUtil.HBASE_EXTENSION_CF);

            loadDf();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void loadDf(){

        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = StaticConfig.javaSc.newAPIHadoopRDD(
                config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaPairRDD<Map, Row> rowKeyPairRDD = hbaseRDD.mapToPair(
                new PairFunction<Tuple2<ImmutableBytesWritable, Result>, Map, Row>() {
                    Map<String, String> fieldMap = new HashMap<String, String>();

                    public Tuple2<Map, Row> call(
                            Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {

                        Result result = entry._2();
                        String keyRow = Bytes.toString(result.getRow());
                        Map<String, String> map = new HashMap<String, String>();
                        List<String> values = new ArrayList<String>();
                        for (KeyValue kv : result.raw()) {
                            values.add(new String(kv.getValue()));
                            fieldMap.put(new String(kv.getFamily()) + HbaseKeyUtil.conStr + new String(kv.getQualifier()), null);
                        }

                        // define java bean
                        return new Tuple2<Map, Row>(fieldMap, RowFactory.create(values));
                    }
                });

        JavaRDD<Row> javaRdd = rowKeyPairRDD.values();

        //get fields of log
        Map fieldsMap = rowKeyPairRDD.keys().reduce(
                new Function2<Map, Map, Map>() {
                    public Map call(Map map, Map map2) throws Exception {
                        Map<String, String> newMap = new HashMap<String, String>();
                        newMap.putAll(map);
                        newMap.putAll(map2);
                        return newMap;
                    }
                }
        );

        dataframe = StaticConfig.sqlContext.createDataFrame(javaRdd, genDfSchema(fieldsMap));
        dataframe.coalesce(1).cache();
        dataframe.registerTempTable(dfTableName);
    }

    private static StructType genDfSchema(Map fieldMap){
        StructType schema = new StructType();
        List<StructField> list = new ArrayList<StructField>();

        Iterator it = fieldMap.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry map = (Map.Entry) it.next();
            StructField field = new StructField((String) map.getKey(), DataTypes.StringType, true, Metadata.empty());
            list.add(field);
        }

        return new StructType(list.toArray(new StructField[list.size()]));
    }

    public DataFrame getDataframe() {
        return dataframe;
    }

    public void registerClasses(Kryo kryo) {
        kryo.register(ImmutableBytesWritable.class);
        kryo.register(Result.class);
    }
}
