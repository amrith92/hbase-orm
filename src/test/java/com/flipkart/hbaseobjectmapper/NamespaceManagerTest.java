package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.testcases.daos.EmployeeDAO;
import com.flipkart.hbaseobjectmapper.testcases.daos.reactive.CitizenDAO;
import com.flipkart.hbaseobjectmapper.testcases.entities.Citizen;
import com.flipkart.hbaseobjectmapper.testcases.entities.Employee;

import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.function.Supplier;

class NamespaceManagerTest {

    private NamespaceManager namespaceManagerUnderTest;

    @Mock
    private AsyncConnection asyncConnection;

    @Mock
    private Connection connection;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
        namespaceManagerUnderTest = new NamespaceManager();
    }

    @Test
    void testConfigureAsyncDao() {
        // Setup
        final Supplier<ReactiveHBDAO<?, ?>> daoCreator = () -> new CitizenDAO(asyncConnection);

        // Run the test
        namespaceManagerUnderTest.configureAsyncDao(Citizen.class, daoCreator);

        // Verify the results
        final ReactiveHBDAO<String, Citizen> dao = namespaceManagerUnderTest.getAsyncDaoFor("test", Citizen.class);
        Assertions.assertNotNull(dao);
        Assertions.assertEquals("test:citizens", dao.getTableName());
    }

    @Test
    void testConfigureDao() {
        // Setup
        final Supplier<AbstractHBDAO<?, ?>> daoCreator = () -> {
            try {
                return new EmployeeDAO(connection);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        };

        // Run the test
        namespaceManagerUnderTest.configureDao(Employee.class, daoCreator);

        // Verify the results
        Assertions.assertNotNull(namespaceManagerUnderTest.getDaoFor("test", Employee.class));
    }

    @Test
    void testGetAsyncDaoFor_ShouldFail_When_NoDaoCreatorConfigured() {
        // Setup

        // Run the test & Verify the results
        Assertions.assertThrows(IllegalStateException.class,
                () -> namespaceManagerUnderTest.getAsyncDaoFor("test", Employee.class));
    }

    @Test
    void testGetDaoFor_ShouldFail_When_NoDaoCreatorConfigured() {
        // Setup

        // Run the test & Verify the results
        Assertions.assertThrows(IllegalStateException.class,
                () -> namespaceManagerUnderTest.getDaoFor("test", Citizen.class));
    }
}
