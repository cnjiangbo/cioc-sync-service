package com.cioc.sync.entity;

import java.util.Date;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import lombok.Data;

@Document(collection = "SyncEliaMarketingTask")
@Data
public class SyncEliaMarketingTask {
    @Id
    private String id;
    private String apiId;
    private String indexField;
    private Date createTime;
    private Date lastSyncTime;
    private long syncedDataCount;
    private long lastSyncedDataCount;
    private int syncCount;
    private String sortMethod;
    // elia yuso
    private String source;
    private boolean enable;
    private String collectionName;

}
