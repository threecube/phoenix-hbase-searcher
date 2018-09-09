package com.threecube.phoenix;

import com.threecube.phoenix.result.LogSearchResult;
import com.threecube.phoenix.writer.bean.HbaseFileWriter;
import com.threecube.phoenix.writer.bean.HbaseViewCreator;
import com.threecube.phoenix.writer.bean.JsonFileWriter;

/**
 * 用于比较创建索引前后，查询数据到hbase table中的效率
 *
 * Created by Mike Ding on 2015/12/24.
 */
public class PutDataStarter extends StarterBase{

    public static void main(String[] args) {
        LogSearchResult result = new LogSearchResult();
        HbaseFileWriter writer = new HbaseFileWriter();
        writer.serJsonFileWriter(new JsonFileWriter(false));
        writer.setHbaseViewCreator(new HbaseViewCreator(false));

        String filePath = "/a2d/data/device_detector_jlog/logout_server_linux/2015-12-22";

        // with local index
        //phoenixReader.execute("create index kerId_evtType_evtTime_inx on detector_info(EVENT_TIME,EVENT_TYPE,GW__KERBEROS_ID)");
        phoenixReader.execute("create local index evtTime_inx on detector_info(EVENT_TIME)");
        phoenixReader.execute("create local index evtType_inx on detector_info(EVENT_TYPE)");
        phoenixReader.execute("create local index kerId_inx on detector_info(GW__KERBEROS_ID)");
        long startTime1 = System.currentTimeMillis();
        writer.run(filePath);
        long endTime1 = System.currentTimeMillis();
        phoenixReader.execute("drop index evtTime_inx on detector_info");
        phoenixReader.execute("drop index evtType_inx on detector_info");
        phoenixReader.execute("drop index kerId_inx on detector_info");
        System.out.println("time (with index) to insert data : " + (endTime1 - startTime1));

//        // without local index
//        long startTime = System.currentTimeMillis();
//        writer.run(filePath);
//        long endTime = System.currentTimeMillis();
//        System.out.println("time (no index) to insert data : " + (endTime - startTime));
    }

}
