package com.threecube.phoenix;

import com.threecube.phoenix.reader.phoenix.PhoenixReader;
import com.threecube.phoenix.result.LogSearchResult;

/**
 * Hello world!
 *
 */
public class LogSearchStarter
{
    public static void main( String[] args )
    {
//            List<String> columns = result.getColumns();
//            List<String> values = result.getValues().get(90689);
//            for(int j =0 ;j< columns.size() ; j++){
//                System.out.print(columns.get(j));
//                System.out.print(" :: ");
//                System.out.println(values.get(j));
//            }

        PhoenixReader phoenixReader = new PhoenixReader();
        LogSearchResult result = new LogSearchResult();

//        phoenixReader.query("select * from detector_info limit 1", result);
//        List<String> columns = result.getColumns();
//            List<String> values = result.getValues().get(0);
//            for(int j =0 ;j< columns.size() ; j++){
//                System.out.print(columns.get(j));
//                System.out.print(" :: ");
//                System.out.println(values.get(j));
//            }

//        long start2Time = System.currentTimeMillis();
//        phoenixReader.query("select 1 from detector_info where EVENT_TIME > '2015-11-20 10:36:55'", result);
//        long end2Time = System.currentTimeMillis();
//        System.out.print(result.getLogNum());
//        System.out.print("  ");
//        System.out.println("time spent 2: " + (end2Time - start2Time));
//
//
//        long start3Time = System.currentTimeMillis();
//        phoenixReader.query("select 1 from detector_info where event_type = 'login_nsa'", result);
//        long end3Time = System.currentTimeMillis();
//        System.out.print(result.getLogNum());
//        System.out.print("  ");
//        System.out.println("time spent 3 : " + (end3Time - start3Time));

        LogSearchResult result1 = new LogSearchResult();
        long start4Time = System.currentTimeMillis();
        phoenixReader.query("select 1 from detector_info group by event_type, event_time order by event_time desc", result1);
        long end4Time = System.currentTimeMillis();
        System.out.print(result1.getLogNum());
        System.out.print("  ");
        System.out.println("time spent 4 : " + (end4Time - start4Time));
    }

}
