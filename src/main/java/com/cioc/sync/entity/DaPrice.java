package com.cioc.sync.entity;

import java.util.Date;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import lombok.Data;

@Document(collection = "DaPrice")
@Data
public class DaPrice {
    @Id
    private String id;

    private String area;

    private Date deliveryStart;

    private Date deliveryEnd;

    private Double price;
}
