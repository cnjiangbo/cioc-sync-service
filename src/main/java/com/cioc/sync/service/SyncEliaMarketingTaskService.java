package com.cioc.sync.service;

import java.util.List;

import com.cioc.sync.entity.SyncEliaMarketingTask;

public interface SyncEliaMarketingTaskService {

    public List<SyncEliaMarketingTask> getAllTasks();

    public SyncEliaMarketingTask getTaskById(String id);

    public SyncEliaMarketingTask createOrUpdateTask(SyncEliaMarketingTask task);

    public void deleteTaskById(String id);
}
