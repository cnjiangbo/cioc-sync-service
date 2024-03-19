package com.cioc.sync.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import com.cioc.sync.entity.SyncEliaMarketingTask;

@Repository
public interface SyncEliaMarketingTaskRepository extends MongoRepository<SyncEliaMarketingTask, String> {
    @Query("{ 'collectionName' : ?0 }")
    SyncEliaMarketingTask findByCollectionName(String collectionName);
}
