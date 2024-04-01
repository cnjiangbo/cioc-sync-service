package com.cioc.sync.entity;

import java.util.Date;

import lombok.Data;

@Data
public class SyncEliaMarketingTask {
    private String id;
    private String apiId;
    private String indexField;
    private Date createTime;
    private Date lastSyncTime;
    private long syncedDataCount;
    private long lastSyncedDataCount;
    private int syncCount;
    private String sortMethod;
    private String source;
    private boolean enable;
    private String collectionName;
    private Integer taskGroup;

}
