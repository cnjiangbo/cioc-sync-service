package com.cioc.sync.jobs.threads;

import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.cioc.sync.entity.SyncEliaMarketingTask;
import com.cioc.sync.jobs.SyncEliaMarketingData;
import com.cioc.sync.service.MongoDataService;

import cn.hutool.core.net.URLEncodeUtil;
import cn.hutool.extra.spring.SpringUtil;
import cn.hutool.http.HttpUtil;

public class EliaSyncThread extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(EliaSyncThread.class);

    private SyncEliaMarketingTask syncEliaMarketingTask;

    String apiPageUrl = "https://opendata.elia.be/explore/dataset/ods#INDEX#/information/";

    String apiDataUrl = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets/#METHOD#/records?order_by=datetime%20asc&limit=100&timezone=UTC";

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
        apiDataUrl = apiDataUrl.replace("#METHOD#", "ods" + formattedNumber);
        mongoDataService.checkAndCreateCollection(collectionName, syncEliaMarketingTask.getIndexField());
        JSONObject lastData = mongoDataService.findLastInsertedDocument(collectionName);

        String where = "";
        if (lastData != null) {
            // 根据最后一次数据的日期查询
            where = "&where=" + syncEliaMarketingTask.getIndexField() + ">'"
                    + lastData.getString(syncEliaMarketingTask.getIndexField()) + "'";
            where = URLEncodeUtil.encode(where);
        }
        String requestURI = apiDataUrl + where;
        logger.debug(requestURI);
        JSONObject object = JSONObject.parseObject(HttpUtil.get(requestURI));
        logger.debug("Get the data with API ID " + syncEliaMarketingTask.getApiId() + " from Elia " + object);
        // 一直更新最后时间，并且每次获取100个数据，直到total_count为0
        Integer totalCount = object.getInteger("total_count");
        Integer totalCountSaveToDb = 0;
        Integer totalError = 0;
        while (totalCount > 0) {
            JSONArray array = object.getJSONArray("results");
            List<JSONObject> jsonObjectList = JSON.parseObject(array.toJSONString(),
                    new TypeReference<List<JSONObject>>() {
                    });
            Integer successCount = mongoDataService.insertDocumentsIgnoreErrors(jsonObjectList, collectionName);
            totalCountSaveToDb += successCount;
            Integer errorCount = array.size() - successCount;
            totalError += errorCount;
            if (errorCount > 0) {
                logger.error(
                        errorCount + " errors occurred when inserting data with API ID "
                                + syncEliaMarketingTask.getApiId()
                                + " of Elia ");
                logger.error("error data is > " + array);
            }
            // 获取最后一个数据，使用时间作为下一次请求的参数
            lastData = array.getObject(array.size() - 1, JSONObject.class);
            where = "";
            where = "&where=" + syncEliaMarketingTask.getIndexField() + ">'"
                    + lastData.getString(syncEliaMarketingTask.getIndexField()) + "'";
            where = URLEncodeUtil.encode(where);
            String url = apiDataUrl + where;
            object = JSONObject.parseObject(HttpUtil.get(url));
            totalCount = object.getInteger("total_count");
        }

        logger.info("complete data sync with API ID " + syncEliaMarketingTask.getApiId() + " success "
                + totalCountSaveToDb + " error " + totalError);
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
