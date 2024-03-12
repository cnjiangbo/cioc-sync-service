package com.cioc.sync.service.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cioc.sync.entity.SyncEliaMarketingTask;
import com.cioc.sync.marketing.tso.elia.service.impl.DataServiceImpl;
import com.cioc.sync.repository.SyncEliaMarketingTaskRepository;
import com.cioc.sync.service.SyncEliaMarketingTaskService;

@Service
public class SyncEliaMarketingTaskServiceImpl implements SyncEliaMarketingTaskService {

    private static final Logger logger = LoggerFactory.getLogger(DataServiceImpl.class);

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
        logger.info("createOrUpdateTask: > " + task);
        return syncEliaMarketingTaskRepository.save(task);
    }

    @SuppressWarnings("null")
    public void deleteTaskById(String id) {
        syncEliaMarketingTaskRepository.deleteById(id);
    }

}
