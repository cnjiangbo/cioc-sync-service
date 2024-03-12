package com.cioc.sync.jobs;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.cioc.sync.entity.SyncEliaMarketingTask;
import com.cioc.sync.marketing.tso.elia.service.impl.DataServiceImpl;
import com.cioc.sync.service.SyncEliaMarketingTaskService;

@Component
public class SyncEliaMarketingData {

    private static final Logger logger = LoggerFactory.getLogger(DataServiceImpl.class);
    @Autowired
    SyncEliaMarketingTaskService syncEliaMarketingTaskService;

    @Scheduled(fixedRate = 5000)
    public void initSyncTask() {
        List<SyncEliaMarketingTask> syncEliaMarketingTasks = syncEliaMarketingTaskService.getAllTasks();
        for (SyncEliaMarketingTask syncEliaMarketingTask : syncEliaMarketingTasks) {
            logger.debug("find task" + syncEliaMarketingTask);

        }
    }

}
