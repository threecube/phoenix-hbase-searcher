package com.threecube.phoenix;

/**
 * Created by Mike Ding on 2015/12/24.
 */
public class LocalIndexStarter1 extends StarterBase{
    public static void main(String[] args){

        String indexSql = "create local index kerId_evtType_evtTime_inx on " +
                "detector_info(EVENT_TIME,EVENT_TYPE,GW__KERBEROS_ID)";
        String dropIndex = "drop index kerId_evtType_evtTime_inx on detector_info";
        String searchByEvtTime = "select event_time, event_type, uuid from detector_info where event_time > '2015-12-20 00:00:00'";

        String searchByEvtType = "select event_time, event_type, uuid from detector_info where event_type = 'login_nsa'";

        String searchByEvetTimeANdType = "select event_time, event_type, uuid from detector_info where event_time > '2015-12-20 00:00:00' " +
                "and EVENT_TYPE='login_nsa'";

        String searchByEvetTimeANdType1 = "select event_time, event_type, uuid from detector_info where EVENT_TYPE='login_nsa' " +
                "and event_time > '2015-12-20 00:00:00'";


        String searchGroup = "select GW__KERBEROS_ID, EVENT_TYPE, count(*) from detector_info " +
                "where event_time > '2015-12-20 00:00:00' group by GW__KERBEROS_ID, EVENT_TYPE";

        System.out.println("time 1 (before) : " + executeAndPrint(searchByEvtTime));
        System.out.println("time 2 (before) : " + executeAndPrint(searchByEvtType));
        System.out.println("time 3 (before) : " + executeAndPrint(searchByEvetTimeANdType));
        System.out.println("time 4 (before) : " + executeAndPrint(searchByEvetTimeANdType1));
        System.out.println("time 5 (before) : " + executeAndPrint(searchGroup));


        phoenixReader.execute("create index evtTime_inx on detector_info(EVENT_TIME)");
        phoenixReader.execute("create index evtType_inx on detector_info(EVENT_TYPE)");
        phoenixReader.execute("create index kerId_inx on detector_info(GW__KERBEROS_ID)");
        phoenixReader.execute("drop index evtTime_inx on detector_info");
        phoenixReader.execute("drop index evtType_inx on detector_info");
        phoenixReader.execute("drop index kerId_inx on detector_info");
        System.out.println("time 1 (after) : " + executeAndPrint(searchByEvtTime));
        System.out.println("time 2 (after) : " + executeAndPrint(searchByEvtType));
        System.out.println("time 3 (after) : " + executeAndPrint(searchByEvetTimeANdType));
        System.out.println("time 4 (after) : " + executeAndPrint(searchByEvetTimeANdType1));
        System.out.println("time 5 (after) : " + executeAndPrint(searchGroup));

    }
}
