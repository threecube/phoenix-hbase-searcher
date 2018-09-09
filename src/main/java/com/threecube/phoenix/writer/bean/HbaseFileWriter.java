package com.threecube.phoenix.writer.bean;

import com.threecube.phoenix.writer.util.ConstentsUtil;
import com.threecube.phoenix.writer.util.HbaseKeyUtil;
import com.threecube.phoenix.writer.model.HbaseRowModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
/**
 * Created by Mike Ding on 2015/11/16.
 */
public class HbaseFileWriter{

    //<clusterName, <columName, value>>
    private JsonFileWriter jsonFileWriter = null;

    private List<HbaseRowModel> buffer = new ArrayList<HbaseRowModel>();

    private HbaseViewCreator hbaseViewCreator = null;

    public void run(String filePath){
        try{
            Configuration conf = new Configuration();
            conf.set("fs.default.name", ConstentsUtil.hdfs);
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(filePath);
            hdfsData2Hbase(fs, path);
            hbaseViewCreator.createView();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public void hdfsData2Hbase(FileSystem fs, Path path){
        BufferedReader br = null;
        try{
            if(fs.isFile(path)){
                if(path.getParent().getParent().getName().equals("fail_server_linux")  &&
                        path.getParent().getName().equals("2015-12-01")){
                    return;
                }
                System.out.println("starting to read file [" + path.getParent().getParent().getName() + "/"
                        + path.getParent().getName() + "/" + path.getName() + "]");
                buffer.clear();
                br=new BufferedReader(new InputStreamReader(fs.open(path)));
                String tempStr = null;
                while((tempStr = br.readLine()) != null){
                    Object jsonMap = jsonToMap(tempStr);
                    HbaseRowModel model;
                    if((model = parserJson(jsonMap)) != null){
                        buffer.add(model);
                    }
                }
                jsonFileWriter.write(buffer);
            }else{
                FileStatus[] inputFiles = fs.listStatus(path);
                for(FileStatus file : inputFiles) {
                    hdfsData2Hbase(fs, file.getPath());
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try {
                if(br != null) {
                    br.close();
                }
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    public HbaseRowModel parserJson(Object jsonMap){
        HbaseRowModel model = new HbaseRowModel();
        Map globalMap = new HashMap<String, String>();
        Map eventMap =  new HashMap<String, String>();
        Map extensionMap =  new HashMap<String, String>();

        if(!(jsonMap instanceof  Map)){
            return null;
        }

        Iterator it = ((Map) jsonMap).entrySet().iterator();
        while(it.hasNext()){
            Map.Entry entry = (Map.Entry)it.next();
            String key = (String)entry.getKey();
            Object value = entry.getValue();

            if(value instanceof Map){
                if(key.equals("EVENT_DATA")){
                    genColumns(eventMap, null, value);
                    hbaseViewCreator.setEventKey(eventMap.keySet().toArray());
                }else if(key.equals("EXTENSION_DATA")){
                    genColumns(extensionMap, null, value);
                    hbaseViewCreator.setExtKey(extensionMap.keySet().toArray());
                }
            }else{
                globalMap.put(key, (String) value);
                hbaseViewCreator.setGlobalKey(key);
                if(key.equals("UUID")){
                    model.setRowKey((String)value);
                }
            }
        }

        //model.setRowKey(HbaseKeyUtil.genRowKey(eventMap, (String) globalMap.get("EVENT_TIME")));
        model.setGlobalCf(globalMap);
        model.setEventCf(eventMap);
        model.setExtensionCf(extensionMap);
        return model;
    }

    public Object jsonToMap(String str) {
        JSONObject jsonObject = null;
        Map<String, Object> map = new HashMap<String, Object>();

        try {
            jsonObject = new JSONObject(str);
            if (jsonObject == null) {
                return null;
            }

            Iterator iterator = jsonObject.keys();
            while (iterator.hasNext()) {
                String key = ((String) iterator.next());
                String value = jsonObject.getString(key);
                map.put(key.toUpperCase(), jsonToMap(value));
            }
        } catch (JSONException e) {
            return str;
        }

        return map;
    }

    // object is map<key, map<String, String>>
    private void genColumns(Map map, String key, Object value){

        if(value instanceof String){
            map.put(key, value);
        }else if(value instanceof Map){
            Iterator itVa = ((Map) value).entrySet().iterator();
            while(itVa.hasNext()) {
                Map.Entry entry = (Map.Entry) itVa.next();
                String keyChild = (String)entry.getKey();
                String newKey = (key == null) ? keyChild : (key + HbaseKeyUtil.conStr +keyChild);
                genColumns(map, newKey, entry.getValue());
            }
        }else{
            return;
        }
    }

    public void serJsonFileWriter(JsonFileWriter jsonFileWriter){
        this.jsonFileWriter = jsonFileWriter;
    }

    public void setHbaseViewCreator(HbaseViewCreator hbaseViewCreator){
        this.hbaseViewCreator = hbaseViewCreator;
    }
}
