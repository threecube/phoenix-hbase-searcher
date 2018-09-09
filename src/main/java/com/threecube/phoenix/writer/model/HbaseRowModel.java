package com.threecube.phoenix.writer.model;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by Mike Ding on 2015/11/16.
 */
public class HbaseRowModel implements Serializable{

    // row key : "eventTime#eventType"
    private String rowKey;

    private Map<String, String> globalCf;

    private Map<String, String> eventCf;

    private Map<String, String> extensionCf;

    public String getRowKey() {
        return rowKey;
    }

    public Map<String, String> getGlobalCf() {
        return globalCf;
    }

    public Map<String, String> getEventCf() {
        return eventCf;
    }

    public Map<String, String> getExtensionCf() {
        return extensionCf;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public void setGlobalCf(Map<String, String> globalCf) {
        this.globalCf = globalCf;
    }

    public void setEventCf(Map<String, String> eventCf) {
        this.eventCf = eventCf;
    }

    public void setExtensionCf(Map<String, String> extensionCf) {
        this.extensionCf = extensionCf;
    }
}
