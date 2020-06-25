package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.*;

@HBTable(name = "blah")
public class ClassWithFieldNotMappedToHBColumn implements HBRecord<String> {

    private String key;

    @HBColumn(family = "f1", column = "c1")
    private Float f1;

    @HBColumn(family = "f1", column = "c1")
    private Double f2;

    @Override
    public String composeRowKey() {
        return key;
    }

    @Override
    public void parseRowKey(String rowKey) {
        this.key = rowKey;
    }
}
