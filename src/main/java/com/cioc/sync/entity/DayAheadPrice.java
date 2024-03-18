package com.cioc.sync.entity;

import java.util.Date;

import lombok.Data;

@Data

public class DayAheadPrice {
    private String area;

    private Date deliveryStart;

    private Date deliveryEnd;

    private Double price;
}
