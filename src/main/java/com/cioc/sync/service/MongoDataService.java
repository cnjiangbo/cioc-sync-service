package com.cioc.sync.service;

import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public interface MongoDataService {
    void saveData(JSON json);

    boolean connectionExists(String collectionName);

    void checkAndCreateCollection(String collectionName, String indexName);

    JSONObject findLastInsertedDocument(String collectionName);

    long countDocuments(String collectionName);

    Integer insertDocumentsIgnoreErrors(List<JSONObject> jsonObjects, String collectionName);

    boolean checkColumnValuesExist(String collectionName, Map<String, Object> columnValues);

    Long countFieldOccurrences(String collectionName, String fieldName, String valueToMatch);

}
