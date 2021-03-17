package com.flipkart.hbaseobjectmapper;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

class AsyncHBAdmin implements HBAdmin {

    private final AsyncConnection connection;

    AsyncHBAdmin(final AsyncConnection connection) {
        this.connection = connection;
    }

    @Override
    public void createNamespace(String namespace) {
        connection.getAdmin().createNamespace(
                NamespaceDescriptor.create(namespace).build())
                .join();
    }

    @Override
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void createTable(final String aNamespace, final Class<T> hbRecordClass) throws IOException {
        WrappedHBTable<R, T> wrappedHBTable = new WrappedHBTable<>(hbRecordClass);
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(wrappedHBTable.getName(aNamespace));
        for (Map.Entry<String, Integer> e : wrappedHBTable.getFamiliesAndVersions().entrySet()) {
            tableDescriptorBuilder.setColumnFamily(
                    ColumnFamilyDescriptorBuilder.newBuilder(
                            Bytes.toBytes(e.getKey())
                    ).setMaxVersions(e.getValue()).build());
        }
        connection.getAdmin().createTable(tableDescriptorBuilder.build())
                .join();
    }

    @Override
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void createTable(Class<T> hbRecordClass) throws IOException {
        createTable(null, hbRecordClass);
    }

    @Override
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void deleteTable(Class<T> hbRecordClass) throws IOException {
        deleteTable(null, hbRecordClass);
    }

    @Override
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void deleteTable(final String aNamespace, final Class<T> hbRecordClass) throws IOException {
        WrappedHBTable<R, T> wrappedHBTable = new WrappedHBTable<>(hbRecordClass);
        connection.getAdmin().deleteTable(wrappedHBTable.getName(aNamespace))
                .join();
    }

    @Override
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void enableTable(Class<T> hbRecordClass) throws IOException {
        enableTable(null, hbRecordClass);
    }

    @Override
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void enableTable(final String aNamespace, final Class<T> hbRecordClass) throws IOException {
        WrappedHBTable<R, T> wrappedHBTable = new WrappedHBTable<>(hbRecordClass);
        connection.getAdmin().enableTable(wrappedHBTable.getName(aNamespace))
                .join();
    }

    @Override
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void disableTable(Class<T> hbRecordClass) throws IOException {
        disableTable(null, hbRecordClass);
    }

    @Override
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void disableTable(final String aNamespace, final Class<T> hbRecordClass) throws IOException {
        WrappedHBTable<R, T> wrappedHBTable = new WrappedHBTable<>(hbRecordClass);
        connection.getAdmin().disableTable(wrappedHBTable.getName(aNamespace))
                .join();
    }

    @Override
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> boolean tableExists(Class<T> hbRecordClass) throws IOException {
        return tableExists(null, hbRecordClass);
    }

    @Override
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> boolean tableExists(final String aNamespace, final Class<T> hbRecordClass) throws IOException {
        WrappedHBTable<R, T> wrappedHBTable = new WrappedHBTable<>(hbRecordClass);
        return connection.getAdmin().tableExists(wrappedHBTable.getName(aNamespace))
                .join();
    }
}
