package com.cioc.sync.service.impl;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.stereotype.Service;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.result.InsertManyResult;

import org.bson.Document;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cioc.sync.service.MongoDataService;
import com.mongodb.DBObject;

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
            mongoTemplate.indexOps(collectionName)
                    .ensureIndex(new Index().on(indexName, Direction.ASC));
            mongoTemplate.indexOps(collectionName)
                    .ensureIndex(new Index().on("signature", Direction.ASC).unique());
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

        List<String> signatures = jsonObjects.stream()
                .map(jsonObject -> jsonObject.getString("signature"))
                .collect(Collectors.toList());
        Query query = new Query();
        query.addCriteria(Criteria.where("signature").in(signatures));
        long alreadyInDataBase = mongoTemplate.count(query, collectionName);

        List<Document> documents = jsonObjects.stream()
                .map(jsonObject -> jsonObject.toJSONString())
                .map(jsonString -> Document.parse((String) jsonString))
                .collect(Collectors.toList());
        InsertManyOptions options = new InsertManyOptions().ordered(false);
        try {
            mongoTemplate.getCollection(collectionName)
                    .insertMany(documents, options);
        } catch (Exception e) {
            // logger.error("There are some wrong with insert many data " + collectionName +
            // " data: " + jsonObjects,
            // e.getMessage());
            // e.printStackTrace();
        }
        return jsonObjects.size() - Integer.valueOf(alreadyInDataBase + "");
    }

    @SuppressWarnings("null")
    @Override
    public boolean checkColumnValuesExist(String collectionName, Map<String, Object> columnValues) {
        if (StrUtil.isBlank(collectionName)) {
            throw new RuntimeException("collectionName can not be null");
        }
        if (columnValues == null || columnValues.size() == 0) {
            throw new RuntimeException("columnValues can not be null");
        }
        Document query = new Document();
        for (Map.Entry<String, Object> entry : columnValues.entrySet()) {
            query.append(entry.getKey(), entry.getValue());
        }

        MongoCursor<Document> cursor = mongoTemplate.getCollection(collectionName).find(query).iterator();
        try {
            return cursor.hasNext();
        } finally {
            cursor.close();
        }
    }

    @SuppressWarnings("null")
    @Override
    public Long countFieldOccurrences(String collectionName, String fieldName, String valueToMatch) {
        if (StrUtil.isBlank(valueToMatch) || StrUtil.isBlank(valueToMatch)) {
            throw new RuntimeException("valueToMatch and valueToMatch can not be null");
        }
        if (StrUtil.isBlank(collectionName)) {
            throw new RuntimeException("collectionName can not be null");
        }

        Document query = new Document(fieldName, valueToMatch);
        return mongoTemplate.getCollection(collectionName).countDocuments(query);
    }

}
