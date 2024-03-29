package com.cioc.sync.jobs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.support.PeriodicTrigger;
import org.springframework.stereotype.Component;

import com.cioc.sync.entity.SyncEliaMarketingTask;
import com.cioc.sync.jobs.threads.EliaSyncThread;
import com.cioc.sync.service.SyncEliaMarketingTaskService;

@Component
public class SyncEliaMarketingData implements SchedulingConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(SyncEliaMarketingData.class);
    @Autowired
    SyncEliaMarketingTaskService syncEliaMarketingTaskService;

    public static List<String> RUNNING_TASK = new ArrayList<>();

    @Value("${e2x.config.sync.taskGroup}")
    private Integer taskGroup;

    @Value("${e2x.config.sync.sync_interval_seconds}")
    private long syncIntervalSeconds;

    @SuppressWarnings({ "deprecation", "null" })
    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.addTriggerTask(() -> {
            initSyncTask();
        }, new PeriodicTrigger(syncIntervalSeconds, TimeUnit.SECONDS));
    }

    public void initSyncTask() {
        List<SyncEliaMarketingTask> syncEliaMarketingTasks = syncEliaMarketingTaskService.getAllTasks();
        for (SyncEliaMarketingTask syncEliaMarketingTask : syncEliaMarketingTasks) {
            if (!syncEliaMarketingTask.isEnable()) {
                continue;
            }
            if (taskGroup != syncEliaMarketingTask.getTaskGroup()) {
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
