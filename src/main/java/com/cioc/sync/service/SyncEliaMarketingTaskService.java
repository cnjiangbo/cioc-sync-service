package com.cioc.sync.service;

import java.util.List;

import com.cioc.sync.entity.SyncEliaMarketingTask;

public interface SyncEliaMarketingTaskService {

    List<SyncEliaMarketingTask> getAllTasks();

    SyncEliaMarketingTask getTaskById(String id);

    SyncEliaMarketingTask createOrUpdateTask(SyncEliaMarketingTask task);

    void deleteTaskById(String id);

    SyncEliaMarketingTask findByCollectionName(String collectionName);
}
