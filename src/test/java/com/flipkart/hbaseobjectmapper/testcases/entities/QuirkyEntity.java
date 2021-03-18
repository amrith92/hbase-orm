package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.DynamicQualifier;
import com.flipkart.hbaseobjectmapper.Family;
import com.flipkart.hbaseobjectmapper.HBDynamicColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBTable;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

@ToString
@EqualsAndHashCode
@HBTable(namespace = "test", name = "yolo", families = {
        @Family(name = "d")
})
public class QuirkyEntity implements HBRecord<String> {
    private String id;
    @HBDynamicColumn(family = "d", prefix = "1", qualifier = @DynamicQualifier(parts = {"key"}))
    private List<KV> kvs;

    @HBDynamicColumn(family = "d", prefix = "2", qualifier = @DynamicQualifier(parts = {"key"}))
    private List<KV2> kv2s;

    @Override
    public String composeRowKey() {
        return id;
    }

    @Override
    public void parseRowKey(final String rowKey) {
        this.id = rowKey;
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public List<KV> getKvs() {
        return kvs;
    }

    public void setKvs(final List<KV> kvs) {
        this.kvs = kvs;
    }

    public List<KV2> getKv2s() {
        return kv2s;
    }

    public void setKv2s(final List<KV2> kv2s) {
        this.kv2s = kv2s;
    }

    public static class KV implements Serializable {
        private String key;
        private String value;

        public KV() {}

        public KV(final String key, final String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public void setKey(final String key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(final String value) {
            this.value = value;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final KV kv = (KV) o;
            return getKey().equals(kv.getKey()) && getValue().equals(kv.getValue());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getKey(), getValue());
        }
    }

    public static class KV2 implements Serializable {
        private String key;
        private String value;
        public KV2() {}

        public KV2(final String key, final String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public void setKey(final String key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(final String value) {
            this.value = value;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final KV2 kv2 = (KV2) o;
            return getKey().equals(kv2.getKey()) && getValue().equals(kv2.getValue());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getKey(), getValue());
        }
    }
}
