package com.threecube.phoenix.writer.util;

import java.util.Map;

/**
 * Created by Mike Ding on 2015/11/18.
 */
public class HbaseKeyUtil {

    private final static String event_nsa_id = "NSA__NSA_ID";

    private final static String event_hostname = "SERVER__HOSTNAME";

    private final static String event_connMehtod = "SERVER__CONNECT_METHOD";

    private final static String event_time = "EVENT_TIME";

    public final static String conStr = "__";

    private final static String strForNull = "NULL";


    public static String genRowKey(Map eventMap, String event_time){
        String rowKey = "";
        if(event_time == null || event_time.length() <=0){
            event_time = strForNull;
        }
        rowKey += event_time;
        rowKey += conStr;

        if(eventMap.get(event_nsa_id) != null){
            rowKey += ((String)eventMap.get(event_nsa_id));
        }else{
            rowKey += strForNull;
        }
        rowKey += conStr;

        if(eventMap.get("event_hostname") != null){
            rowKey+=((String)eventMap.get(event_hostname));
        }else{
            rowKey += strForNull;
        }
        rowKey += conStr;

        if(eventMap.get(event_connMehtod) != null){
            rowKey += ((String)eventMap.get(event_connMehtod));
        }else{
            rowKey += strForNull;
        }


        return rowKey;
    }
}
