package com.cioc.sync.service.impl;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.stereotype.Service;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.result.InsertManyResult;

import org.bson.Document;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cioc.sync.service.MongoDataService;
import com.mongodb.DBObject;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.InsertManyOptions;

import cn.hutool.core.util.StrUtil;

@Service
public class MongoDataServiceImpl implements MongoDataService {

    @Autowired
    private MongoTemplate mongoTemplate;
    private static final Logger logger = LoggerFactory.getLogger(MongoDataServiceImpl.class);

    @Override
    public void saveData(JSON json) {

    }

    @SuppressWarnings("null")
    @Override
    public boolean connectionExists(String collectionName) {
        if (StrUtil.isBlank(collectionName)) {
            throw new RuntimeException("collectionName can not be null.");
        }
        return mongoTemplate.collectionExists(collectionName);
    }

    @SuppressWarnings("null")
    @Override
    public void checkAndCreateCollection(String collectionName, String indexName) {
        if (StrUtil.isBlank(collectionName)) {
            throw new RuntimeException("collectionName can not be null.");
        }
        if (StrUtil.isBlank(indexName)) {
            throw new RuntimeException("indexName can not be null.");
        }
        if (!mongoTemplate.collectionExists(collectionName)) {
            mongoTemplate.createCollection(collectionName);
            logger.info("Collection '" + collectionName + "' created successfully.");
            // create index field
            mongoTemplate.indexOps(collectionName).ensureIndex(new Index().on(indexName, Direction.ASC).unique());
            logger.info("Index '" + indexName + "' created successfully.");
        } else {
            logger.debug("Collection '" + collectionName + "' already exists.");
        }
    }

    @SuppressWarnings("null")
    @Override
    public JSONObject findLastInsertedDocument(String collectionName) {
        if (StrUtil.isBlank(collectionName)) {
            throw new RuntimeException("collectionName can not be null.");
        }
        Query query = new Query();
        query.with(Sort.by(Sort.Direction.DESC, "_id")).limit(1);
        DBObject dbObject = mongoTemplate.findOne(query, DBObject.class, collectionName);
        if (dbObject != null) {
            String jsonString = JSON.toJSONString(dbObject);
            JSONObject jsonObject = JSONObject.parseObject(jsonString);
            return jsonObject;
        } else {
            return null;
        }
    }

    @SuppressWarnings("null")
    @Override
    public long countDocuments(String collectionName) {
        if (StrUtil.isBlank(collectionName)) {
            throw new RuntimeException("collectionName can not be null.");
        }
        return mongoTemplate.count(null, collectionName);
    }

    @SuppressWarnings("null")
    @Override
    public Integer insertDocumentsIgnoreErrors(List<JSONObject> jsonObjects, String collectionName) {
        List<Document> documents = jsonObjects.stream()
                .map(jsonObject -> jsonObject.toJSONString())
                .map(jsonString -> Document.parse((String) jsonString))
                .collect(Collectors.toList());
        InsertManyOptions options = new InsertManyOptions().ordered(false);
        InsertManyResult result = null;
        try {
            result = mongoTemplate.getCollection(collectionName)
                    .insertMany(documents, options);
        } catch (Exception e) {
            // e.printStackTrace();
        }

        return result == null ? 0 : result.getInsertedIds().size();
    }

}
