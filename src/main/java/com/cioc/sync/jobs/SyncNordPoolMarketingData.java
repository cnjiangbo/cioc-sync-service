package com.cioc.sync.jobs;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;

@Component
public class SyncNordPoolMarketingData {
    private static final Logger logger = LoggerFactory.getLogger(SyncNordPoolMarketingData.class);

    @Scheduled(fixedRate = 5000)
    public void syncDaPrice() {
        String tomorrowStr = DateUtil.format(DateUtil.offsetDay(new Date(), 1), "yyyy-MM-dd");
        String url = "https://dataportal-api.nordpoolgroup.com/api/DayAheadPrices?date=#DATE#&market=DayAhead&deliveryArea=BE&currency=EUR";
        // 查询数据库有没有存储明日电价，没有则同步
    }

    public static void main(String[] args) {

    }

}
