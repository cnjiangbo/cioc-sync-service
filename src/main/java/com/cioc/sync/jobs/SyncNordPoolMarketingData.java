package com.cioc.sync.jobs;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cioc.sync.entity.DaPrice;
import com.cioc.sync.service.DaPriceService;

import cn.hutool.core.date.DateUtil;
import cn.hutool.http.HttpUtil;

@Component
public class SyncNordPoolMarketingData {
    private static final Logger logger = LoggerFactory.getLogger(SyncNordPoolMarketingData.class);
    @Autowired
    DaPriceService daPriceService;

    @Scheduled(cron = "0 40 15 * * ?")
    public void syncDaPrice() {
        String area = "BE";
        // 当前时间14点后发布
        String tomorrowStr = DateUtil.format(DateUtil.offsetDay(new Date(), 1), "yyyy-MM-dd");
        String url = "https://dataportal-api.nordpoolgroup.com/api/DayAheadPrices?date=" + tomorrowStr
                + "&market=DayAhead&deliveryArea=BE&currency=EUR";
        JSONObject object = JSONObject.parseObject(HttpUtil.get(url));
        if (object.containsKey("deliveryDateCET") && tomorrowStr.equals(object.getString("deliveryDateCET"))) {
            JSONArray array = object.getJSONArray("multiAreaEntries");
            for (int i = 0; i < array.size(); i++) {
                JSONObject element = array.getJSONObject(i);
                DaPrice daPrice = new DaPrice();
                daPrice.setArea(area);
                daPrice.setDeliveryStart(element.getDate("deliveryStart"));
                daPrice.setDeliveryEnd(element.getDate("deliveryEnd"));
                daPrice.setPrice(element.getJSONObject("entryPerArea").getDouble(area));
                daPriceService.createOrUpdateTask(daPrice);
            }
            logger.info("sync da price success " + area);
        } else {
            logger.error("request nordpool API error " + object);
        }
    }

    public static void main(String[] args) {

    }

}
