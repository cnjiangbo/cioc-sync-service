package com.cioc.sync.jobs.threads;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cioc.sync.entity.SyncEliaMarketingTask;
import com.cioc.sync.jobs.SyncEliaMarketingData;
import com.cioc.sync.service.MongoDataService;

import cn.hutool.extra.spring.SpringUtil;
import cn.hutool.http.HttpUtil;

public class EliaSyncThread extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(EliaSyncThread.class);

    private SyncEliaMarketingTask syncEliaMarketingTask;

    String apiPageUrl = "https://opendata.elia.be/explore/dataset/ods#INDEX#/information/";

    String apiData = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets/#METHOD#/records?order_by=datetime%20asc&limit=#LIMIT#&timezone=UTC";

    public EliaSyncThread(SyncEliaMarketingTask syncEliaMarketingTask) {
        this.syncEliaMarketingTask = syncEliaMarketingTask;
    }

    @Override
    public void run() {
        MongoDataService mongoDataService = SpringUtil.getBean(MongoDataService.class);
        logger.info("Start new task >" + syncEliaMarketingTask);
        String formattedNumber = String.format("%03d", Integer.parseInt(syncEliaMarketingTask.getApiId()));
        String result = HttpUtil.get(apiPageUrl.replace("#INDEX#", formattedNumber));
        Document doc = Jsoup.parse(result);
        String collectionName = getCollectionName(doc.title());
        logger.debug("Get collection name from page title > " + collectionName);
        apiData = apiData.replace("#METHOD#", "ods" + formattedNumber);
        mongoDataService.checkAndCreateCollection(collectionName, syncEliaMarketingTask.getIndexField());
        // Integer limit = getLimit(collectionName);
        // apiData = apiData.replace("#LIMIT#", limit + "");

        logger.info("End task thread > " + syncEliaMarketingTask);
        SyncEliaMarketingData.RUNNING_TASK.remove(syncEliaMarketingTask.getId());
    }

    private String getCollectionName(String pageTitle) {
        pageTitle = pageTitle.toLowerCase().trim();
        pageTitle = pageTitle.replace(" — elia open data portal", "");
        pageTitle = pageTitle.replace("(", "");
        pageTitle = pageTitle.replace(")", "");
        pageTitle = pageTitle.replaceAll("\\s+", "-");
        return pageTitle;
    }

}
