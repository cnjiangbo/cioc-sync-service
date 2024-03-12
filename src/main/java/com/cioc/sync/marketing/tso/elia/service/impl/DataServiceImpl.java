package com.cioc.sync.marketing.tso.elia.service.impl;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cioc.sync.marketing.tso.elia.service.DataService;

import cn.hutool.core.util.StrUtil;

@Service
public class DataServiceImpl implements DataService {

    @Autowired
    private MongoTemplate mongoTemplate;
    private static final Logger logger = LoggerFactory.getLogger(DataServiceImpl.class);

    @Override
    public void saveData(JSON json) {
        logger.info("test");
        JSONObject object = new JSONObject();
        object.put("test", new Date());
        mongoTemplate.save(object, "test-bob");

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
            mongoTemplate.indexOps(collectionName).ensureIndex(new Index().on(indexName, Direction.ASC));
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
        return JSONObject
                .parseObject(JSONObject.toJSONString(mongoTemplate.findOne(query, JSONObject.class, collectionName)));
    }

    @SuppressWarnings("null")
    @Override
    public long countDocuments(String collectionName) {
        if (StrUtil.isBlank(collectionName)) {
            throw new RuntimeException("collectionName can not be null.");
        }
        return mongoTemplate.count(null, collectionName);
    }

}
