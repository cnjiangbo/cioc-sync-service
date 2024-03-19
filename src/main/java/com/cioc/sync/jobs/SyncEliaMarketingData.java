package com.cioc.sync.jobs;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.cioc.sync.entity.SyncEliaMarketingTask;
import com.cioc.sync.jobs.threads.EliaSyncThread;
import com.cioc.sync.service.SyncEliaMarketingTaskService;

@Component
public class SyncEliaMarketingData {

    private static final Logger logger = LoggerFactory.getLogger(SyncEliaMarketingData.class);
    @Autowired
    SyncEliaMarketingTaskService syncEliaMarketingTaskService;

    public static List<String> RUNNING_TASK = new ArrayList<>();

    @Scheduled(fixedRate = 1000 * 60 * 20)
    public void initSyncTask() {
        List<SyncEliaMarketingTask> syncEliaMarketingTasks = syncEliaMarketingTaskService.getAllTasks();
        for (SyncEliaMarketingTask syncEliaMarketingTask : syncEliaMarketingTasks) {
            if (!syncEliaMarketingTask.isEnable()) {
                continue;
            }
            logger.debug("Find task > " + syncEliaMarketingTask);
            if (RUNNING_TASK.contains(syncEliaMarketingTask.getId())) {
                // 不重复执行
                logger.debug("task is running " + syncEliaMarketingTask);
                continue;
            }
            if (syncEliaMarketingTask.getSource().equals("elia")) {
                new EliaSyncThread(syncEliaMarketingTask).start();
                RUNNING_TASK.add(syncEliaMarketingTask.getId());
            }
        }
    }

}
