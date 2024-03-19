package com.cioc.sync.jobs.threads;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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

import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.digest.MD5;
import cn.hutool.extra.spring.SpringUtil;
import cn.hutool.http.HttpUtil;
import lombok.Data;

public class EliaSyncThread extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(EliaSyncThread.class);

    private SyncEliaMarketingTask syncEliaMarketingTask;

    private static String LIMITED_DAY = null;

    String apiPageUrl = "https://opendata.elia.be/explore/dataset/ods#INDEX#/information/";

    public EliaSyncThread(SyncEliaMarketingTask syncEliaMarketingTask) {
        this.syncEliaMarketingTask = syncEliaMarketingTask;
    }

    @Override
    public void run() {
        try {

            if (LIMITED_DAY != null) {
                // 判断是不是新的一天，如果是则置为false
                if (LIMITED_DAY.equals(getDateStr())) {
                    logger.debug("Trigger traffic limit");
                    endThisTask();
                    return;
                } else {
                    LIMITED_DAY = null;
                }
            }
            MongoDataService mongoDataService = SpringUtil.getBean(MongoDataService.class);

            logger.info("Start new task >" + syncEliaMarketingTask);

            String collectionName = getCollectionName();
            logger.debug("Get collection name from page title > " + collectionName);

            mongoDataService.checkAndCreateCollection(collectionName, syncEliaMarketingTask.getIndexField());

            JSONObject lastData = mongoDataService.findLastInsertedDocument(collectionName);

            String requestURI = getUrl(syncEliaMarketingTask, null);

            if (lastData != null) {
                requestURI = getUrl(syncEliaMarketingTask,
                        lastData.getString(syncEliaMarketingTask.getIndexField()).replace("+00:00", ""));
            }

            EliaResponse eliaResponse = requestAPI(requestURI);
            if (!eliaResponse.isSuccess()) {
                // 请求API出错了
                endThisTask();
                // 如果是触发流量限制，那么加入内存 避免当日下次请求
                if ("10005".equals(eliaResponse.getErrorCode())) {
                    LIMITED_DAY = getDateStr();
                    logger.error("Trigger traffic limit first time", eliaResponse);
                }
                return;
            }

            Integer totalCount = eliaResponse.getDataCount();

            Integer totalCountSaveToDb = 0;
            Integer totalError = 0;
            while (totalCount > 0) {
                JSONArray array = eliaResponse.getData();
                List<JSONObject> jsonObjectList = JSON.parseObject(array.toJSONString(),
                        new TypeReference<List<JSONObject>>() {
                        });
                List<JSONObject> objectsToInsertIntoDatabase = new ArrayList<>();
                for (JSONObject object : jsonObjectList) {
                    String sign = generateMD5FromJSONObject(object);
                    object.put("signature", sign);
                    objectsToInsertIntoDatabase.add(object);
                }
                // }
                Integer successCount = 0;
                if (objectsToInsertIntoDatabase.size() > 0) {
                    successCount = mongoDataService.insertDocumentsIgnoreErrors(objectsToInsertIntoDatabase,
                            collectionName);
                }

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

                saveSyncRecord(successCount, errorCount, totalCount, collectionName);

                lastData = array.getObject(array.size() - 1, JSONObject.class);
                String url = getUrl(syncEliaMarketingTask,
                        lastData.getString(syncEliaMarketingTask.getIndexField()).replace("+00:00", ""));
                ;
                logger.debug("request url in while loop " + url);

                eliaResponse = requestAPI(url);
                if (!eliaResponse.isSuccess()) {
                    break;
                }
                totalCount = eliaResponse.getDataCount();
                logger.info("The number of APIs with ID " + syncEliaMarketingTask.getApiId()
                        + " waiting for synchronized data is " + totalCount);

            }

            logger.info("Complete data sync with API ID " + syncEliaMarketingTask.getApiId() + " success "
                    + totalCountSaveToDb + " error " + totalError);

        } catch (Exception e) {
            logger.error("there are something wrong with elia API " + syncEliaMarketingTask.getApiId(),
                    e);
        }
        endThisTask();
    }

    void endThisTask() {
        logger.info("End task thread > " + syncEliaMarketingTask);
        SyncEliaMarketingData.RUNNING_TASK.remove(syncEliaMarketingTask.getId());
    }

    void saveSyncRecord(Integer successCount, Integer errorCount, Integer totalCount, String collectionName) {
        SyncRecordService syncRecordService = SpringUtil.getBean(SyncRecordService.class);
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
    }

    String generateMD5FromJSONObject(JSONObject jsonObject) {
        // 使用 TreeMap 来按照键的首字母排序
        Map<String, Object> sortedMap = new TreeMap<>(jsonObject);
        // 构造键值对字符串
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry : sortedMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            sb.append(key).append("=").append(value).append("&");
        }

        // 去除末尾的 "&" 符号
        String keyValueString = sb.toString().replaceAll("&$", "");
        return MD5.create().digestHex(keyValueString);
    }

    private String getCollectionName() {
        String formattedNumber = String.format("%03d", Integer.parseInt(syncEliaMarketingTask.getApiId()));
        String result = HttpUtil.get(apiPageUrl.replace("#INDEX#", formattedNumber));
        Document doc = Jsoup.parse(result);
        String pageTitle = doc.title();
        pageTitle = pageTitle.toLowerCase().trim();
        pageTitle = pageTitle.replace(" — elia open data portal", "");
        pageTitle = pageTitle.replace("(", "");
        pageTitle = pageTitle.replace(")", "");
        pageTitle = pageTitle.replaceAll("\\s+", "-");
        return pageTitle;
    }

    public void getAllEliaApi(Integer index) {
        String url = "https://opendata.elia.be/explore/dataset/ods#INDEX#/information/";
        List<String> apis = new ArrayList<>();
        for (int i = 1; i < 999; i++) {
            String formattedNumber = String.format("%03d", i);
            String result = HttpUtil.get(url.replace("#INDEX#", formattedNumber));
            if (result.indexOf("Page not found") > 0) {
                continue;
            } else {
                Document doc = Jsoup.parse(result);
                String name = "INDEX " + i + "\t" + doc.title();
                apis.add(name);
            }
        }
        for (String line : apis) {
            System.out.println(line);
        }
    }

    /**
     * 
     * @param api
     * @return
     */
    public EliaResponse requestAPI(String apiUrl) {
        EliaResponse eliaResponse = new EliaResponse();
        try {
            JSONObject object = JSONObject.parseObject(HttpUtil.get(apiUrl));
            logger.debug("Get the data with API ID " + apiUrl + " " + syncEliaMarketingTask.getApiId() + " from Elia "
                    + object);

            if (object.containsKey("error")) {
                // Get error information from Elia
                eliaResponse.setSuccess(false);
                eliaResponse.setErrorCode(object.getString("errorcode"));
                logger.error("Get wrong message from Elia", object);
            } else {
                eliaResponse.setSuccess(true);
                eliaResponse.setDataCount(object.getInteger("total_count"));
                eliaResponse.setData(object.getJSONArray("results"));
            }
        } catch (Exception exception) {
            eliaResponse.setSuccess(false);
            logger.error("request " + apiUrl + " error", exception);
        }
        return eliaResponse;
    }

    public String getDateStr() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(new Date());
    }

    String getUrl(SyncEliaMarketingTask task, String lastDateTime) {
        String url = null;
        try {
            String baseUrl = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets/ods"
                    + String.format("%03d", Integer.parseInt(syncEliaMarketingTask.getApiId())) + "/records";

            String orderBy = task.getIndexField() + " asc";
            int limit = 100;
            String timezone = "UTC";

            url = baseUrl + "?order_by=" + orderBy +
                    "&limit=" + limit +
                    "&timezone=" + timezone;
            if (!StrUtil.isBlank(lastDateTime)) {
                String whereClause = task.getIndexField() + ">'" + lastDateTime + "'";
                String encodedWhereClause = URLEncoder.encode(whereClause, "UTF-8");
                url = url + "&where=" + encodedWhereClause;
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return url;

    }

    public static void main(String[] args) {
        EliaSyncThread thread = new EliaSyncThread(null);
        String baseUrl = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets/ods001/records";
        String whereClause = "datetime>'2015-01-01T23:45:00'";
        String orderBy = "datetime asc";
        int limit = 100;
        String timezone = "UTC";

        try {
            // 对 where 子句进行 URL 编码
            String encodedWhereClause = URLEncoder.encode(whereClause, "UTF-8");

            // 构建完整的 URL
            String url = baseUrl + "?order_by=" + orderBy +
                    "&limit=" + limit +
                    "&timezone=" + timezone +
                    "&where=" + encodedWhereClause;

            // 输出 URL
            System.out.println("Encoded URL: " + url.equals(
                    "https://opendata.elia.be/api/explore/v2.1/catalog/datasets/ods001/records?order_by=datetime asc&limit=100&timezone=UTC&where=datetime%3E%272015-01-01T23%3A45%3A00%27"));
            System.out.println(HttpUtil.get(url));
        } catch (UnsupportedEncodingException e) {
            // 处理不支持的编码异常
            e.printStackTrace();
        }
    }

}

@Data
class EliaResponse {
    private boolean success;
    private Integer dataCount;
    private JSONArray data;
    private String errorCode;

}
