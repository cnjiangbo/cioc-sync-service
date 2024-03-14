package com.cioc.sync.jobs.threads;

import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.Date;
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
import com.cioc.sync.entity.SyncRecord;
import com.cioc.sync.jobs.SyncEliaMarketingData;
import com.cioc.sync.service.MongoDataService;
import com.cioc.sync.service.SyncEliaMarketingTaskService;
import com.cioc.sync.service.SyncRecordService;

import cn.hutool.core.net.URLEncodeUtil;
import cn.hutool.extra.spring.SpringUtil;
import cn.hutool.http.HttpUtil;

public class EliaSyncThread extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(EliaSyncThread.class);

    private SyncEliaMarketingTask syncEliaMarketingTask;

    String apiPageUrl = "https://opendata.elia.be/explore/dataset/ods#INDEX#/information/";

    String apiDataUrl = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets/#METHOD#/records?order_by=#ORDER_BY# asc&limit=100&timezone=UTC&";

    public EliaSyncThread(SyncEliaMarketingTask syncEliaMarketingTask) {
        this.syncEliaMarketingTask = syncEliaMarketingTask;
    }

    @Override
    public void run() {
        try {

            MongoDataService mongoDataService = SpringUtil.getBean(MongoDataService.class);
            SyncRecordService syncRecordService = SpringUtil.getBean(SyncRecordService.class);
            logger.info("Start new task >" + syncEliaMarketingTask);
            String formattedNumber = String.format("%03d", Integer.parseInt(syncEliaMarketingTask.getApiId()));
            String result = HttpUtil.get(apiPageUrl.replace("#INDEX#", formattedNumber));
            Document doc = Jsoup.parse(result);
            String collectionName = getCollectionName(doc.title());
            logger.debug("Get collection name from page title > " + collectionName);
            apiDataUrl = apiDataUrl.replace("#METHOD#", "ods" + formattedNumber);
            apiDataUrl = apiDataUrl.replace("#ORDER_BY#", syncEliaMarketingTask.getIndexField());
            mongoDataService.checkAndCreateCollection(collectionName, syncEliaMarketingTask.getIndexField());
            JSONObject lastData = mongoDataService.findLastInsertedDocument(collectionName);

            String where = "";
            if (lastData != null) {
                // 根据最后一次数据的日期查询
                where = "where=" + syncEliaMarketingTask.getIndexField() + ">'"
                        + lastData.getString(syncEliaMarketingTask.getIndexField()).replace("+00:00", "") + "'";
            }
            String requestURI = apiDataUrl + where;
            logger.debug(requestURI);
            JSONObject object = JSONObject.parseObject(HttpUtil.get(requestURI));
            logger.debug("Get the data with API ID " + syncEliaMarketingTask.getApiId() + " from Elia " + object);
            // 一直更新最后时间，并且每次获取100个数据，直到total_count为0
            Integer totalCount = 0;
            try {
                totalCount = object.getInteger("total_count");
            } catch (Exception e) {
                // 请求API出现错误
                e.printStackTrace();
                logger.error("request elia API  with ID " + syncEliaMarketingTask.getApiId() + " error", e);
            }
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

                // 保存同步记录
                SyncRecord syncRecord = new SyncRecord();
                syncRecord.setSuccessCount(successCount);
                syncRecord.setErrorCount(errorCount);
                syncRecord.setApiId(syncEliaMarketingTask.getApiId());
                syncRecord.setSyncTime(new Date());
                syncRecord.setRequestDataCount(totalCount);
                syncRecordService.createOrUpdateTask(syncRecord);

                // 更新总记录
                syncEliaMarketingTask.setLastSyncTime(new Date());
                syncEliaMarketingTask.setLastSyncedDataCount(successCount);
                syncEliaMarketingTask.setSyncCount(syncEliaMarketingTask.getSyncCount() + 1);
                syncEliaMarketingTask.setSyncedDataCount(syncEliaMarketingTask.getSyncedDataCount() + successCount);
                syncEliaMarketingTask.setCollectionName(collectionName);
                SpringUtil.getBean(SyncEliaMarketingTaskService.class).createOrUpdateTask(syncEliaMarketingTask);

                // 获取最后一个数据，使用时间作为下一次请求的参数
                lastData = array.getObject(array.size() - 1, JSONObject.class);
                where = "";
                where = "&where=" + syncEliaMarketingTask.getIndexField() + ">'"
                        + lastData.getString(syncEliaMarketingTask.getIndexField()).replace("+00:00", "") + "'";
                where = URLEncodeUtil.encode(where);
                String url = apiDataUrl + where;
                logger.debug("request url in while loop " + url);
                object = JSONObject.parseObject(HttpUtil.get(url));
                totalCount = object.getInteger("total_count");
                logger.info("The number of APIs with ID " + syncEliaMarketingTask.getApiId()
                        + " waiting for synchronized data is " + totalCount);

            }

            logger.info("Complete data sync with API ID " + syncEliaMarketingTask.getApiId() + " success "
                    + totalCountSaveToDb + " error " + totalError);
            logger.info("End task thread > " + syncEliaMarketingTask);
        } catch (Exception e) {
            logger.error("there are something wrong with elia API " + syncEliaMarketingTask.getApiId(), e);
        }
        SyncEliaMarketingData.RUNNING_TASK.remove(syncEliaMarketingTask.getId());
    }

    void updateTask(Integer successCount) {

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
