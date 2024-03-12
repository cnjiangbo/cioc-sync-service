package com.cioc.sync.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cioc.sync.entity.SyncEliaMarketingTask;
import com.cioc.sync.service.SyncEliaMarketingTaskService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@RestController
@RequestMapping("/syncEliaMarketingTaskController")
public class SyncEliaMarketingTaskController {

    @Autowired
    SyncEliaMarketingTaskService syncEliaMarketingTaskService;

    @GetMapping("/all")
    public List<SyncEliaMarketingTask> getAllTasks() {
        return syncEliaMarketingTaskService.getAllTasks();
    }

    @PostMapping("/createOrUpdateTask")
    public String saveOrUpdate(@RequestBody SyncEliaMarketingTask syncEliaMarketingTask) {
        syncEliaMarketingTaskService.createOrUpdateTask(syncEliaMarketingTask);
        return "success";
    }

}
