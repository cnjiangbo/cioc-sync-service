package com.cioc.sync.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cioc.sync.entity.SyncRecord;
import com.cioc.sync.repository.SyncRecordRepository;
import com.cioc.sync.service.SyncRecordService;

@Service
public class SyncRecordServiceImpl implements SyncRecordService {

    @Autowired
    SyncRecordRepository syncRecordRepository;

    // private static final Logger logger =
    // LoggerFactory.getLogger(MongoDataServiceImpl.class);

    @SuppressWarnings("null")
    @Override
    public SyncRecord createOrUpdateTask(SyncRecord record) {
        return syncRecordRepository.save(record);
    }

}
