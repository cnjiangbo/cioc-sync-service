package com.cioc.sync.marketing.tso.elia.service;

import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

@Service
public interface DataService {
    void saveData(JSON json);

    boolean connectionExists(String collectionName);

    void checkAndCreateCollection(String collectionName, String indexName);

    JSONObject findLastInsertedDocument(String collectionName);

    long countDocuments(String collectionName);

}
