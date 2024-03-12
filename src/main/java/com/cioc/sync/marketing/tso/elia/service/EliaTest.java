package com.cioc.sync.marketing.tso.elia.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.hutool.http.HttpUtil;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.alibaba.fastjson2.JSONObject;

public class EliaTest {

    private static Map<String, Long> SYNC_RECORD = new HashMap<>();
    public static Integer TOTAL = 0;

    public void syncDataViaApi(Integer index) {
        String apiPageUrl = "https://opendata.elia.be/explore/dataset/ods#INDEX#/information/";
        String apiData = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets/#METHOD#/records?order_by=datetime%20asc&limit=#LIMIT#&timezone=UTC";
        // String apiData =
        // "https://opendata.elia.be/api/explore/v2.1/catalog/datasets/#METHOD#/records";

        String formattedNumber = String.format("%03d", index);
        String result = HttpUtil.get(apiPageUrl.replace("#INDEX#", formattedNumber));
        Document doc = Jsoup.parse(result);
        String collectionName = getCollectionName(doc.title());
        System.out.println("表名称:\t" + collectionName);
        Integer limit = getLimit(collectionName);
        apiData = apiData.replace("#METHOD#", "ods" + formattedNumber);
        apiData = apiData.replace("#LIMIT#", limit + "");
        requestAndSaveToMongodb(apiData, collectionName);

    }

    public String getCollectionName(String title) {
        title = title.toLowerCase().trim();
        title = title.replace(" — elia open data portal", "");
        title = title.replace("(", "");
        title = title.replace(")", "");
        title = title.replaceAll("\\s+", "-");
        return title;
    }

    private void requestAndSaveToMongodb(String url, String collectionName) {
        String result = HttpUtil.get(url);
        System.out.println(url);
        JSONObject object = JSONObject.parseObject(result);
        int dataCount = object.getInteger("total_count");
        if (dataCount > 0) {
            // 查找最后一次记录,根据datetime排序。 请求API查询大于最后时间的数据，并开始分页读取并存储 如果最后一次数据为空，那么就不需要根据时间检索数据

            // JSONArray array = object.getJSONArray("results");
            // for (int i = 0; i < array.size(); i++) {
            // JSONObject data = array.getJSONObject(i);
            //// System.out.println(data);
            // //插入mongodb
            // }
            System.out.println("collectionName:" + collectionName + "\t" + "dataCount:" + dataCount);
            TOTAL += dataCount;
        }

    }

    /**
     * 在系统启动时，尽量同步更多的数据，Elia的最大限制是100
     * 在后续运行时，最多同步两条即可，因为所有的数据都是每分钟同步一次
     *
     * @param collectionName
     * @return
     */
    private Integer getLimit(String collectionName) {
        if (SYNC_RECORD.containsKey(collectionName)) {
            return 2;
        } else {
            return 100;
        }
    }

    public void getAllEliaApi() {
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

    public static void main(String[] args) {
        List<Integer> apiIndex = new ArrayList<>();
        apiIndex.add(1);
        // apiIndex.add(2);
        // apiIndex.add(31);
        // apiIndex.add(32);
        // apiIndex.add(45);
        // apiIndex.add(46);
        // apiIndex.add(47);
        // apiIndex.add(61);
        // apiIndex.add(62);
        // apiIndex.add(63);
        // apiIndex.add(64);
        // apiIndex.add(68);
        // apiIndex.add(69);
        // apiIndex.add(77);
        // apiIndex.add(78);
        // apiIndex.add(79);
        // apiIndex.add(80);
        // apiIndex.add(81);
        // apiIndex.add(82);
        // apiIndex.add(83);
        // apiIndex.add(86);
        // apiIndex.add(87);
        // apiIndex.add(88);
        // apiIndex.add(136);
        // apiIndex.add(139);
        // apiIndex.add(140);
        // apiIndex.add(147);
        for (Integer index : apiIndex) {
            new EliaTest().syncDataViaApi(index);
        }
        System.out.println("总数据量:" + TOTAL);
    }

}
