package com.cioc.sync.service;

import com.cioc.sync.entity.SyncRecord;

public interface SyncRecordService {
    SyncRecord createOrUpdateTask(SyncRecord record);
}
