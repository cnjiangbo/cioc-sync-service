package com.cioc.sync;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import cn.hutool.core.text.csv.CsvUtil;
import cn.hutool.core.text.csv.CsvWriter;
import cn.hutool.core.util.CharsetUtil;
import cn.hutool.http.HttpUtil;
import lombok.Data;

public class NordpoolTest {
    public static void main(String[] args) {

        CsvWriter writer = CsvUtil.getWriter("d://daprice.csv", CharsetUtil.CHARSET_UTF_8);
        writer.write(new String[] { "deliveryStart", "deliveryEnd", "price" });
        for (int i = 1; i < 15; i++) {
            String formattedNumber = String.format("%02d", i);
            String url = "https://dataportal-api.nordpoolgroup.com/api/DayAheadPrices?date=2024-03-" + formattedNumber
                    + "&market=DayAhead&deliveryArea=BE&currency=EUR";
            JSONObject result = JSONObject.parseObject(HttpUtil.get(url));
            JSONArray array = result.getJSONArray("multiAreaEntries");
            for (int j = 0; j < array.size(); j++) {
                JSONObject object = array.getJSONObject(j);
                DaPrice daPrice = new DaPrice();
                daPrice.setDeliveryStart(object.getString("deliveryStart"));
                daPrice.setDeliveryEnd(object.getString("deliveryEnd"));
                daPrice.setPrice(object.getJSONObject("entryPerArea").getDouble("BE"));
                writer.write(
                        new String[] { daPrice.getDeliveryStart(), daPrice.getDeliveryEnd(), daPrice.getPrice() + "" });
            }
        }

    }

}

@Data
class DaPrice {
    private String deliveryStart;
    private String deliveryEnd;
    private Double price;
}