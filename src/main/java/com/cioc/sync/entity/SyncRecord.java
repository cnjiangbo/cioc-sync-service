package com.cioc.sync.entity;

import java.util.Date;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import lombok.Data;

@Document(collection = "SyncRecord")
@Data
public class SyncRecord {
    @Id
    private String id;
    private String apiId;
    private Date syncTime;
    private Integer errorCount;
    private Integer successCount;
    private Integer requestDataCount;
}
