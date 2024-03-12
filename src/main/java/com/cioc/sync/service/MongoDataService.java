package com.cioc.sync.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public interface MongoDataService {
    void saveData(JSON json);

    boolean connectionExists(String collectionName);

    void checkAndCreateCollection(String collectionName, String indexName);

    JSONObject findLastInsertedDocument(String collectionName);

    long countDocuments(String collectionName);

}
