package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.exceptions.AmbiguousColumnDefinitionError;
import com.flipkart.hbaseobjectmapper.exceptions.DuplicateCodecFlagForColumnException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.io.netty.util.internal.StringUtil;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * A wrapper class for {@link HBColumn} and {@link HBColumnMultiVersion} annotations (for internal use only)
 */
class WrappedHBColumn {
    private final String family;
    private final String column;
    private final boolean multiVersioned;
    private final boolean singleVersioned;
    private final Class<? extends Annotation> annotationClass;
    private final Map<String, String> codecFlags;
    private final Field field;
    private String[] parts = null;
    private String qualifierSeparator = null;
    private String separator = null;
    private String prefix = null;
    private byte[] separatorBytes = null;
    private byte[] prefixBytes = null;
    private boolean shouldPreserveOrder = false;
    private Type dynamicType = null;

    WrappedHBColumn(Field field) {
        this.field = field;
        HBColumn hbColumn = field.getAnnotation(HBColumn.class);
        HBDynamicColumn hbDynamicColumn = field.getAnnotation(HBDynamicColumn.class);
        HBColumnMultiVersion hbColumnMultiVersion = field.getAnnotation(HBColumnMultiVersion.class);
        if (hasSpecifiedMoreThanOneAnnotation(hbColumn, hbDynamicColumn, hbColumnMultiVersion)) {
            final Annotation[] ambiguousAnnotations = Stream.of(hbColumn, hbDynamicColumn, hbColumnMultiVersion)
                    .filter(Objects::nonNull)
                    .toArray(Annotation[]::new);
            throw new AmbiguousColumnDefinitionError(field, ambiguousAnnotations);
        }
        if (hbColumn != null) {
            family = hbColumn.family();
            column = hbColumn.column();
            singleVersioned = true;
            multiVersioned = false;
            annotationClass = HBColumn.class;
            codecFlags = toMap(hbColumn.codecFlags());
        } else if (hbColumnMultiVersion != null) {
            family = hbColumnMultiVersion.family();
            column = hbColumnMultiVersion.column();
            singleVersioned = false;
            multiVersioned = true;
            annotationClass = HBColumnMultiVersion.class;
            codecFlags = toMap(hbColumnMultiVersion.codecFlags());
        } else if (hbDynamicColumn != null) {
            if (!Collection.class.isAssignableFrom(field.getType())) {
                throw new IllegalArgumentException("HBDynamicColumn field must be a collection, but was " + field.getType());
            }
            validateQualifierParts(hbDynamicColumn.qualifier());
            validateQualifierField(field, hbDynamicColumn.qualifier());
            Preconditions.checkArgument(!hbDynamicColumn.prefix().isEmpty(), "Prefix of HBDynamicColumn may not be null or empty!");
            Preconditions.checkArgument(!hbDynamicColumn.qualifier().separator().isEmpty(), "Qualifier separator of HBDynamicColumn may not be null or empty!");
            family = hbDynamicColumn.family();
            qualifierSeparator = hbDynamicColumn.qualifier().separator();
            separator = hbDynamicColumn.separator();
            separatorBytes = Bytes.toBytes(separator);
            parts = hbDynamicColumn.qualifier().parts();
            column = String.join(separator, parts);
            prefix = hbDynamicColumn.prefix();
            prefixBytes = Bytes.toBytes(prefix);
            shouldPreserveOrder = hbDynamicColumn.preserveOrder();
            final ParameterizedType collectionType = (ParameterizedType) field.getGenericType();
            dynamicType = collectionType.getActualTypeArguments()[0];
            annotationClass = HBDynamicColumn.class;
            singleVersioned = true;
            multiVersioned = false;
            codecFlags = toMap(hbDynamicColumn.codecFlags());
        } else {
            family = null;
            column = null;
            singleVersioned = false;
            multiVersioned = false;
            annotationClass = null;
            codecFlags = null;
        }
    }

    private void validateQualifierField(final Field field, final DynamicQualifier qualifier) {
        final ParameterizedType listType = (ParameterizedType) field.getGenericType();
        final Type actualTypeArgument = listType.getActualTypeArguments()[0];
        if (actualTypeArgument instanceof Class) {
            final Class<?> qualifierObjectClassType = (Class<?>) actualTypeArgument;
            for (final String part : qualifier.parts()) {
                try {
                    final Field declaredField = qualifierObjectClassType.getDeclaredField(part);
                    if (!declaredField.getType().equals(String.class)) {
                        throw new IllegalArgumentException("The generic Type of HBDynamicColumn must have a field with name " + part + " of type string, but was " + declaredField.getType());
                    }
                } catch (NoSuchFieldException e) {
                    throw new IllegalArgumentException("Generic Type of HBDynamicColumn must have a field with name " + part + " but was not found");
                }
            }
        } else {
            throw new IllegalArgumentException("Generic Type of HBDynamicColumn must be a ParameterizedType");
        }
    }

    private void validateQualifierParts(final DynamicQualifier qualifier) {
        if (qualifier.parts().length == 0) {
            throw new IllegalArgumentException("Parts array of DynamicQualifier cannot be empty or null");
        }
        for (final String part : qualifier.parts()) {
            if (StringUtil.isNullOrEmpty(part)) {
                throw new IllegalArgumentException("A part of DynamicQualifier cannot be empty or null");
            }
        }
    }

    private boolean hasSpecifiedMoreThanOneAnnotation(final HBColumn hbColumn, final HBDynamicColumn hbDynamicColumn, final HBColumnMultiVersion hbColumnMultiVersion) {
        return hbColumn != null && (hbColumnMultiVersion != null || hbDynamicColumn != null) || (hbColumnMultiVersion != null && hbDynamicColumn != null);
    }

    private Map<String, String> toMap(Flag[] codecFlags) {
        Map<String, String> flagsMap = new HashMap<>(codecFlags.length, 1.0f);
        for (Flag flag : codecFlags) {
            String previousValue = flagsMap.put(flag.name(), flag.value());
            if (previousValue != null) {
                throw new DuplicateCodecFlagForColumnException(field.getDeclaringClass(), field.getName(), annotationClass, flag.name());
            }
        }
        return flagsMap;
    }

    public String family() {
        return family;
    }

    public byte[] familyBytes() {
        return Bytes.toBytes(family);
    }

    public String column() {
        return column;
    }

    public byte[] columnBytes() {
        return Bytes.toBytes(column);
    }

    public Map<String, String> codecFlags() {
        return codecFlags;
    }

    public boolean isPresent() {
        return singleVersioned || multiVersioned;
    }

    public boolean isMultiVersioned() {
        return multiVersioned;
    }

    public boolean isSingleVersioned() {
        return singleVersioned;
    }

    public boolean isDynamic() {
        return HBDynamicColumn.class == annotationClass;
    }

    public String[] getParts() {
        return parts;
    }

    public String getQualifierSeparator() {
        return qualifierSeparator;
    }

    public String getSeparator() {
        return separator;
    }

    public byte[] getSeparatorBytes() {
        return separatorBytes;
    }

    public byte[] getPrefixBytes() {
        return prefixBytes;
    }

    public boolean shouldPreserveOrder() {
        return shouldPreserveOrder;
    }

    public Type getDynamicType() {
        return dynamicType;
    }

    public String getName() {
        return annotationClass.getName();
    }

    @Override
    public String toString() {
        if (prefix != null) {
            return String.format("%s:%s%s", family, prefix, separator);
        } else {
            return String.format("%s:%s", family, column);
        }
    }
}
