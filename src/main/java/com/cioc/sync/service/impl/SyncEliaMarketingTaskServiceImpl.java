package com.cioc.sync.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cioc.sync.entity.SyncEliaMarketingTask;
import com.cioc.sync.repository.SyncEliaMarketingTaskRepository;
import com.cioc.sync.service.SyncEliaMarketingTaskService;

@Service
public class SyncEliaMarketingTaskServiceImpl implements SyncEliaMarketingTaskService {

    @Autowired
    SyncEliaMarketingTaskRepository syncEliaMarketingTaskRepository;

    public List<SyncEliaMarketingTask> getAllTasks() {
        return syncEliaMarketingTaskRepository.findAll();
    }

    @SuppressWarnings("null")
    public SyncEliaMarketingTask getTaskById(String id) {
        return syncEliaMarketingTaskRepository.findById(id).orElse(null);
    }

    @SuppressWarnings("null")
    public SyncEliaMarketingTask createOrUpdateTask(SyncEliaMarketingTask task) {
        return syncEliaMarketingTaskRepository.save(task);
    }

    @SuppressWarnings("null")
    public void deleteTaskById(String id) {
        syncEliaMarketingTaskRepository.deleteById(id);
    }

}
