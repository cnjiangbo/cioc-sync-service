package com.cioc.sync.service.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cioc.sync.entity.SyncEliaMarketingTask;
import com.cioc.sync.service.ImportHistoricalDataService;
import com.cioc.sync.service.MongoDataService;
import com.cioc.sync.service.SyncEliaMarketingTaskService;

import cn.hutool.core.io.file.FileReader;
import cn.hutool.core.util.StrUtil;

@Service
public class ImportHistoricalDataServiceImpl implements ImportHistoricalDataService {

    @Autowired
    MongoDataService mongoDataService;
    private static final Logger logger = LoggerFactory.getLogger(ImportHistoricalDataServiceImpl.class);

    @Autowired
    SyncEliaMarketingTaskService syncEliaMarketingTaskService;

    @Override
    public void importJsonFile(String absolutePath, String collectionName, String sort) {
        JSONArray array = JSON.parseArray(FileReader.create(new File(absolutePath)).readString());
        logger.info(array.size() + " collectionName data waiting for import ");
        if (StrUtil.isBlank(sort)) {
            sort = "asc";
        }
        SyncEliaMarketingTask task = syncEliaMarketingTaskService.findByCollectionName(collectionName);
        mongoDataService.checkAndCreateCollection(collectionName, task.getIndexField());
        List<JSONObject> insertData = new ArrayList<>();
        if (sort.equals("desc")) {
            for (int i = array.size() - 1; i >= 0; i--) {
                JSONObject object = array.getJSONObject(i);
                insertData.add(object);
                if (i % 1000 == 0) {
                    mongoDataService.insertDocumentsIgnoreErrors(insertData, collectionName);
                    logger.debug("insert 1000 data to " + collectionName + ", the remaining data count is " + i);
                    insertData = new ArrayList<>();
                }
            }
        } else {
            for (int i = 0; i < array.size(); i++) {
                JSONObject object = array.getJSONObject(i);
                insertData.add(object);
                if (i % 1000 == 0) {
                    mongoDataService.insertDocumentsIgnoreErrors(insertData, collectionName);
                    logger.debug("insert 1000 data to " + collectionName + ", the remaining data count is "
                            + (array.size() - i));
                    insertData = new ArrayList<>();
                }
            }
        }
        if (insertData.size() > 0) {
            mongoDataService.insertDocumentsIgnoreErrors(insertData, collectionName);
        }
        task.setSyncCount(1);
        task.setLastSyncTime(new Date());
        task.setLastSyncedDataCount(array.size());
        task.setSyncCount(array.size());
        syncEliaMarketingTaskService.createOrUpdateTask(task);
        logger.info("Successfully inserted " + array.size() + " pieces of data into " + collectionName);
    }

}
