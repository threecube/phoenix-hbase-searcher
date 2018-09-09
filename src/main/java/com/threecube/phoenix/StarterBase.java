package com.threecube.phoenix;

import com.threecube.phoenix.reader.phoenix.PhoenixReader;
import com.threecube.phoenix.result.LogSearchResult;

import java.util.List;
import java.util.TreeMap;

/**
 * Created by Mike Ding on 2015/12/23.
 */
public class StarterBase {

    protected static PhoenixReader phoenixReader = new PhoenixReader();

    protected static long executeAndPrint(String sql){
        LogSearchResult result = new LogSearchResult();
        long startTime = System.currentTimeMillis();
        phoenixReader.query(sql, result);
        long endTime = System.currentTimeMillis();
        List<String> columns = result.getColumns();
        List<String> values = result.getValues().get(0);
        for(int j =0 ;j< columns.size() ; j++){
            System.out.print(columns.get(j));
            System.out.print(" :: ");
            System.out.println(values.get(j));
        }
        return endTime - startTime;
    }

}
