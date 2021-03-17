package com.flipkart.hbaseobjectmapper.testcases.daos;

import com.flipkart.hbaseobjectmapper.AbstractHBDAO;
import com.flipkart.hbaseobjectmapper.testcases.entities.QuirkyEntity;

import org.apache.hadoop.hbase.client.Connection;

public class QuirkyDAO extends AbstractHBDAO<String, QuirkyEntity> {
    public QuirkyDAO(Connection connection) {
        super(connection);
    }
}
