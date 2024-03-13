package com.cioc.sync.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;
import com.cioc.sync.entity.SyncRecord;

@Repository
public interface SyncRecordRepository extends MongoRepository<SyncRecord, String> {

}
