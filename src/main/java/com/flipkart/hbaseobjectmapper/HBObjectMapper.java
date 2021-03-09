package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.codec.BestSuitCodec;
import com.flipkart.hbaseobjectmapper.codec.Codec;
import com.flipkart.hbaseobjectmapper.codec.exceptions.DeserializationException;
import com.flipkart.hbaseobjectmapper.codec.exceptions.SerializationException;
import com.flipkart.hbaseobjectmapper.exceptions.AllHBColumnFieldsNullException;
import com.flipkart.hbaseobjectmapper.exceptions.BadHBaseLibStateException;
import com.flipkart.hbaseobjectmapper.exceptions.CodecException;
import com.flipkart.hbaseobjectmapper.exceptions.ColumnFamilyNotInHBTableException;
import com.flipkart.hbaseobjectmapper.exceptions.ConversionFailedException;
import com.flipkart.hbaseobjectmapper.exceptions.EmptyConstructorInaccessibleException;
import com.flipkart.hbaseobjectmapper.exceptions.FieldAnnotatedWithHBColumnMultiVersionCantBeEmpty;
import com.flipkart.hbaseobjectmapper.exceptions.FieldsMappedToSameColumnException;
import com.flipkart.hbaseobjectmapper.exceptions.IncompatibleFieldForHBColumnMultiVersionAnnotationException;
import com.flipkart.hbaseobjectmapper.exceptions.InternalError;
import com.flipkart.hbaseobjectmapper.exceptions.MappedColumnCantBePrimitiveException;
import com.flipkart.hbaseobjectmapper.exceptions.MappedColumnCantBeStaticException;
import com.flipkart.hbaseobjectmapper.exceptions.MappedColumnCantBeTransientException;
import com.flipkart.hbaseobjectmapper.exceptions.MissingHBColumnFieldsException;
import com.flipkart.hbaseobjectmapper.exceptions.NoEmptyConstructorException;
import com.flipkart.hbaseobjectmapper.exceptions.ObjectNotInstantiatableException;
import com.flipkart.hbaseobjectmapper.exceptions.RowKeyCantBeComposedException;
import com.flipkart.hbaseobjectmapper.exceptions.RowKeyCantBeEmptyException;
import com.flipkart.hbaseobjectmapper.exceptions.RowKeyCouldNotBeParsedException;
import com.flipkart.hbaseobjectmapper.exceptions.UnsupportedFieldTypeException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * <p>An <b>object mapper class</b> that helps<ol>
 * <li>serialize objects of your <i>bean-like class</i> to HBase's {@link Put} and {@link Result} objects</li>
 * <li>deserialize HBase's {@link Put} and {@link Result} objects to objects of your <i>bean-like class</i></li>
 * </ol>
 * where, your <i>bean-like class</i> is like any other POJO/bean but implements {@link HBRecord} interface.<br>
 * <br>
 * <p>
 * This class is for use in:<ol>
 * <li>MapReduce jobs which <i>read from</i> and/or <i>write to</i> HBase tables</li>
 * <li>Unit-tests</li>
 * </ol>
 * <p><b>This class is thread-safe.</b>
 * <br><br>
 * This class is designed in such a way that only one instance needs to be maintained for the entire lifecycle of your program.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Plain_old_Java_object">POJO</a>
 * @see <a href="https://en.wikipedia.org/wiki/JavaBeans">JavaBeans</a>
 */
public class HBObjectMapper {

    private static final byte[] BYTE_TERMINAL_VALUE = {Byte.MAX_VALUE};
    private final Codec codec;

    /**
     * Instantiate object of this class with a custom {@link Codec}
     *
     * @param codec Codec to be used for serialization and deserialization of fields
     * @see #HBObjectMapper()
     */
    public HBObjectMapper(Codec codec) {
        if (codec == null) {
            throw new IllegalArgumentException("Parameter 'codec' cannot be null. If you want to use the default codec, use the no-arg constructor");
        }
        this.codec = codec;
    }

    /**
     * Instantiate an object of this class with default {@link Codec} (that is, {@link BestSuitCodec})
     *
     * @see #HBObjectMapper(Codec)
     */
    public HBObjectMapper() {
        this(new BestSuitCodec());
    }

    /**
     * Serialize row key
     *
     * @param rowKey Object representing row key
     * @param <R>    Data type of row key
     * @return Byte array
     */
    <R extends Serializable & Comparable<R>> byte[] rowKeyToBytes(R rowKey, Map<String, String> codecFlags) {
        return valueToByteArray(rowKey, codecFlags);
    }

    @SuppressWarnings("unchecked")
    <R extends Serializable & Comparable<R>, T extends HBRecord<R>> R bytesToRowKey(byte[] rowKeyBytes, Map<String, String> codecFlags, Class<T> entityClass) {
        try {
            return (R) byteArrayToValue(rowKeyBytes, entityClass.getDeclaredMethod("composeRowKey").getReturnType(), codecFlags);
        } catch (NoSuchMethodException e) {
            throw new InternalError(e);
        }
    }

    /**
     * Core method that drives deserialization
     *
     * @see #convertRecordToMap(HBRecord)
     */
    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T convertMapToRecord(
            byte[] rowKeyBytes,
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map,
            Class<T> clazz) {
        Collection<Field> fields = getHBColumnFields0(clazz).values();
        WrappedHBTable<R, T> hbTable = new WrappedHBTable<>(clazz);
        R rowKey = bytesToRowKey(rowKeyBytes, hbTable.getCodecFlags(), clazz);
        T record;
        try {
            record = clazz.getDeclaredConstructor()
                    .newInstance();
        } catch (Exception ex) {
            throw new ObjectNotInstantiatableException("Error while instantiating empty constructor of " + clazz.getName(), ex);
        }
        try {
            record.parseRowKey(rowKey);
        } catch (Exception ex) {
            throw new RowKeyCouldNotBeParsedException(String.format("Supplied row key \"%s\" could not be parsed", rowKey), ex);
        }
        for (final Field field : fields) {
            WrappedHBColumn hbColumn = new WrappedHBColumn(field);
            NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = map.get(hbColumn.familyBytes());
            if (familyMap == null || familyMap.isEmpty()) {
                continue;
            }
            if (hbColumn.isDynamic()) {
                convertDynamicColumn(record, field, hbColumn, familyMap);
            } else {
                convertStandardColumn(record, field, hbColumn, familyMap);
            }
        }
        return record;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void convertDynamicColumn(final T record, final Field field, final WrappedHBColumn hbColumn, final NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap) {
        final byte[] prefixBytes = hbColumn.getPrefixBytes();
        final byte[] separatorBytes = hbColumn.getSeparatorBytes();
        final byte[] dynamicPrefix = Bytes.add(prefixBytes, separatorBytes);
        final byte[] dynamicPrefixMax = Bytes.add(dynamicPrefix, BYTE_TERMINAL_VALUE);
        final SortedMap<byte[], NavigableMap<Long, byte[]>> subMap = familyMap.subMap(dynamicPrefix, dynamicPrefixMax);
        if (subMap.size() > 0) {
            final List values = new ArrayList(subMap.size());
            for (final Map.Entry<byte[], NavigableMap<Long, byte[]>> entry : subMap.entrySet()) {
                final Object value = byteArrayToValue(entry.getValue().firstEntry().getValue(), hbColumn.getDynamicType(), hbColumn.codecFlags());
                values.add(value);
            }
            objectSetFieldValue(record, field, values);
        }
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void convertStandardColumn(final T record, final Field field, final WrappedHBColumn hbColumn, final NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap) {
        NavigableMap<Long, byte[]> columnVersionsMap = familyMap.get(hbColumn.columnBytes());
        if (hbColumn.isSingleVersioned()) {
            if (columnVersionsMap == null || columnVersionsMap.isEmpty()) {
                return;
            }
            Map.Entry<Long, byte[]> firstEntry = columnVersionsMap.firstEntry();
            objectSetFieldValue(record, field, firstEntry.getValue(), hbColumn.codecFlags());
        } else {
            objectSetFieldValue(record, field, columnVersionsMap, hbColumn.codecFlags());
        }
    }

    /**
     * Converts a {@link Serializable} object into a <code>byte[]</code>
     *
     * @param value      Object to be serialized
     * @param codecFlags Flags to be passed to Codec
     * @return Byte-array representing serialized object
     * @see #byteArrayToValue(byte[], Type, Map)
     */
    byte[] valueToByteArray(Serializable value, Map<String, String> codecFlags) {
        try {
            return codec.serialize(value, codecFlags);
        } catch (SerializationException e) {
            throw new CodecException("Couldn't serialize", e);
        }
    }

    /**
     * <p>Serialize an object to HBase's {@link ImmutableBytesWritable}.
     * <p>This method is for use in Mappers, unit-tests for Mappers and unit-tests for Reducers.
     *
     * @param value Object to be serialized
     * @return Byte array, wrapped in HBase's data type
     * @see #getRowKey
     */
    public ImmutableBytesWritable toIbw(Serializable value) {
        return new ImmutableBytesWritable(valueToByteArray(value, null));
    }

    <R extends Serializable & Comparable<R>, T extends HBRecord<R>> WrappedHBTable<R, T> validateHBClass(Class<T> clazz) {
        Constructor<?> constructor;
        try {
            constructor = clazz.getDeclaredConstructor();
        } catch (NoSuchMethodException e) {
            throw new NoEmptyConstructorException(clazz, e);
        }
        if (!Modifier.isPublic(constructor.getModifiers())) {
            throw new EmptyConstructorInaccessibleException(String.format("Empty constructor of class %s is inaccessible. It needs to be public.", clazz.getName()));
        }
        int numOfHBColumns = 0;
        WrappedHBTable<R, T> hbTable = new WrappedHBTable<>(clazz);
        Set<FamilyAndColumn> columns = new HashSet<>(clazz.getDeclaredFields().length, 1.0f);
        Map<String, Field> hbColumnFields = getHBColumnFields0(clazz);
        for (Field field : hbColumnFields.values()) {
            WrappedHBColumn hbColumn = new WrappedHBColumn(field);
            if (hbColumn.isPresent()) {
                if (!hbTable.isColumnFamilyPresent(hbColumn.family())) {
                    throw new ColumnFamilyNotInHBTableException(clazz.getName(), field.getName(), hbColumn.family(), hbColumn.column());
                }
                if (hbColumn.isSingleVersioned()) {
                    validateHBColumnSingleVersionField(field);
                } else if (hbColumn.isMultiVersioned()) {
                    validateHBColumnMultiVersionField(field);
                }
                if (!columns.add(new FamilyAndColumn(hbColumn.family(), hbColumn.column()))) {
                    throw new FieldsMappedToSameColumnException(String.format("Class %s has more than one field (e.g. '%s') mapped to same HBase column %s", clazz.getName(), field.getName(), hbColumn));
                }
                numOfHBColumns++;
            }
        }
        if (numOfHBColumns == 0) {
            throw new MissingHBColumnFieldsException(clazz);
        }
        return hbTable;
    }

    /**
     * Internal note: This should be in sync with {@link #getFieldType(Field, boolean)}
     */
    private void validateHBColumnMultiVersionField(Field field) {
        validateHBColumnField(field);
        if (!(field.getGenericType() instanceof ParameterizedType)) {
            throw new IncompatibleFieldForHBColumnMultiVersionAnnotationException(String.format("Field %s is not even a parameterized type", field));
        }
        if (field.getType() != NavigableMap.class) {
            throw new IncompatibleFieldForHBColumnMultiVersionAnnotationException(String.format("Field %s is not a NavigableMap", field));
        }
        ParameterizedType pType = (ParameterizedType) field.getGenericType();
        Type[] typeArguments = pType.getActualTypeArguments();
        if (typeArguments.length != 2 || typeArguments[0] != Long.class) {
            throw new IncompatibleFieldForHBColumnMultiVersionAnnotationException(String.format("Field %s has unexpected type params (Key should be of %s type)", field, Long.class.getName()));
        }
        if (!codec.canDeserialize(getFieldType(field, true))) {
            throw new UnsupportedFieldTypeException(String.format("Field %s in class %s is of unsupported type Navigable<Long,%s> ", field.getName(), field.getDeclaringClass().getName(), field.getDeclaringClass().getName()));
        }
    }

    /**
     * Internal note: For multi-version usecase, this should be in sync with {@link #validateHBColumnMultiVersionField(Field)}
     */
    Type getFieldType(Field field, boolean isMultiVersioned) {
        if (isMultiVersioned) {
            return ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[1];
        } else {
            return field.getGenericType();
        }
    }

    private void validateHBColumnSingleVersionField(Field field) {
        validateHBColumnField(field);
        Type fieldType = getFieldType(field, false);
        if (fieldType instanceof Class) {
            Class<?> fieldClazz = (Class<?>) fieldType;
            if (fieldClazz.isPrimitive()) {
                throw new MappedColumnCantBePrimitiveException(String.format("Field %s in class %s is a primitive of type %s (Primitive data types are not supported as they're not nullable)", field.getName(), field.getDeclaringClass().getName(), fieldClazz.getName()));
            }
        }
        if (!codec.canDeserialize(fieldType)) {
            throw new UnsupportedFieldTypeException(String.format("Field %s in class %s is of unsupported type (%s)", field.getName(), field.getDeclaringClass().getName(), fieldType));
        }
    }

    private void validateHBColumnField(Field field) {
        WrappedHBColumn hbColumn = new WrappedHBColumn(field);
        int modifiers = field.getModifiers();
        if (Modifier.isTransient(modifiers)) {
            throw new MappedColumnCantBeTransientException(field, hbColumn.getName());
        }
        if (Modifier.isStatic(modifiers)) {
            throw new MappedColumnCantBeStaticException(field, hbColumn.getName());
        }
    }

    /**
     * Core method that drives serialization
     *
     * @see #convertMapToRecord(byte[], NavigableMap, Class)
     */
    @SuppressWarnings("unchecked")
    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>>
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> convertRecordToMap(T record) {
        Class<T> clazz = (Class<T>) record.getClass();
        Collection<Field> fields = getHBColumnFields0(clazz).values();
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        int numOfFieldsToWrite = 0;
        for (final Field field : fields) {
            final WrappedHBColumn hbColumn = new WrappedHBColumn(field);
            if (hbColumn.isSingleVersioned()) {
                final byte[] familyName = hbColumn.familyBytes();
                map.computeIfAbsent(familyName, f -> new TreeMap<>(Bytes.BYTES_COMPARATOR));
                final Map<byte[], NavigableMap<Long, byte[]>> columns = map.get(familyName);
                if (hbColumn.isDynamic()) {
                    final Map<byte[], NavigableMap<Long, byte[]>> dynamicColumns = getDynamicColumns(record, field, hbColumn);
                    columns.putAll(dynamicColumns);
                    numOfFieldsToWrite += dynamicColumns.size();
                } else {
                    final byte[] columnName = hbColumn.columnBytes();
                    final byte[] fieldValueBytes = getFieldValueAsBytes(record, field, hbColumn.codecFlags());
                    if (fieldValueBytes != null && fieldValueBytes.length != 0) {
                        NavigableMap<Long, byte[]> singleValue = new TreeMap<>();
                        singleValue.put(HConstants.LATEST_TIMESTAMP, fieldValueBytes);
                        columns.put(columnName, singleValue);
                        numOfFieldsToWrite++;
                    }
                }
            } else if (hbColumn.isMultiVersioned()) {
                NavigableMap<Long, byte[]> fieldValueVersions = getFieldValuesAsNavigableMapOfBytes(record, field, hbColumn.codecFlags());
                if (fieldValueVersions != null) {
                    final byte[] familyName = hbColumn.familyBytes();
                    final byte[] columnName = hbColumn.columnBytes();
                    map.computeIfAbsent(familyName, f -> new TreeMap<>(Bytes.BYTES_COMPARATOR));
                    Map<byte[], NavigableMap<Long, byte[]>> columns = map.get(familyName);
                    columns.put(columnName, fieldValueVersions);
                    numOfFieldsToWrite++;
                }
            }
        }
        if (numOfFieldsToWrite == 0) {
            throw new AllHBColumnFieldsNullException();
        }
        return map;
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> Map<byte[], NavigableMap<Long, byte[]>> getDynamicColumns(final T record, final Field field, final WrappedHBColumn hbColumn) {
        try {
            field.setAccessible(true);
            @SuppressWarnings("rawtypes") final Collection value = (Collection) field.get(record);
            if (value == null) {
                return Collections.emptyMap();
            }
            return getDynamicColumns0(hbColumn, value);
        } catch (IllegalAccessException e) {
            throw new ConversionFailedException("Unable to handle dynamic column!", e);
        }
    }

    Map<byte[], NavigableMap<Long, byte[]>> getDynamicColumns0(final WrappedHBColumn hbColumn, @SuppressWarnings("rawtypes") final Collection value) {
        try {
            final Map<byte[], NavigableMap<Long, byte[]>> columnValues = new TreeMap<>(Bytes.BYTES_COMPARATOR);
            final List<String> parts = Arrays.asList(hbColumn.getParts());
            final byte[] qualifierSeparatorBytes = hbColumn.getQualifierSeparator().getBytes(StandardCharsets.UTF_8);
            int valueIndex = 0;
            for (final Object v : value) {
                final Field[] declaredFields = v.getClass().getDeclaredFields();
                Preconditions.checkArgument(parts.stream().allMatch(part -> Arrays.stream(declaredFields).anyMatch(f -> part.equals(f.getName()))), String.format("Specified parts %s do not exist in class %s!", parts, v.getClass()));
                byte[] partValues = null;
                for (final String part : parts) {
                    final Field partField = v.getClass().getDeclaredField(part);
                    partField.setAccessible(true);
                    final Serializable partValue = (Serializable) partField.get(v);
                    final byte[] serializedPartValue = valueToByteArray(partValue, hbColumn.codecFlags());
                    if (partValues == null) {
                        partValues = serializedPartValue;
                    } else {
                        partValues = Bytes.add(partValues, qualifierSeparatorBytes, serializedPartValue);
                    }
                }
                if (partValues != null) {
                    final byte[] prefix = hbColumn.getPrefixBytes();
                    final byte[] separator = hbColumn.getSeparatorBytes();
                    final byte[] columnName = hbColumn.shouldPreserveOrder()
                            ? Bytes.add(prefix, separator, Bytes.add(Bytes.toBytes(valueIndex), separator, partValues))
                            : Bytes.add(prefix, separator, partValues);
                    final byte[] columnSingleValue = valueToByteArray((Serializable) v, hbColumn.codecFlags());
                    final NavigableMap<Long, byte[]> singleValue = new TreeMap<>();
                    singleValue.put(HConstants.LATEST_TIMESTAMP, columnSingleValue);
                    columnValues.put(columnName, singleValue);
                }

                ++valueIndex;
            }
            return columnValues;
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new ConversionFailedException("Unable to handle dynamic column!", e);
        }
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> byte[] getFieldValueAsBytes(T record, Field field, Map<String, String> codecFlags) {
        Serializable fieldValue;
        try {
            field.setAccessible(true);
            fieldValue = (Serializable) field.get(record);
        } catch (IllegalAccessException e) {
            throw new BadHBaseLibStateException(e);
        }
        return valueToByteArray(fieldValue, codecFlags);
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> NavigableMap<Long, byte[]> getFieldValuesAsNavigableMapOfBytes(T record, Field field, Map<String, String> codecFlags) {
        try {
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            NavigableMap<Long, R> fieldValueVersions = (NavigableMap<Long, R>) field.get(record);
            if (fieldValueVersions == null)
                return null;
            if (fieldValueVersions.size() == 0) {
                throw new FieldAnnotatedWithHBColumnMultiVersionCantBeEmpty();
            }
            NavigableMap<Long, byte[]> output = new TreeMap<>();
            for (Map.Entry<Long, R> e : fieldValueVersions.entrySet()) {
                Long timestamp = e.getKey();
                R fieldValue = e.getValue();
                if (fieldValue == null)
                    continue;
                byte[] fieldValueBytes = valueToByteArray(fieldValue, codecFlags);
                output.put(timestamp, fieldValueBytes);
            }
            return output;
        } catch (IllegalAccessException e) {
            throw new BadHBaseLibStateException(e);
        }
    }

    /**
     * <p>Converts an object of your bean-like class to HBase's {@link Put} object.
     * <p>This method is for use in a MapReduce job whose <code>Reducer</code> class extends HBase's <code>org.apache.hadoop.hbase.mapreduce.TableReducer</code> class (in other words, a MapReduce job whose output is an HBase table)
     *
     * @param record An object of your bean-like class (one that implements {@link HBRecord} interface)
     * @param <R>    Data type of row key
     * @param <T>    Entity type
     * @return HBase's {@link Put} object
     */
    @SuppressWarnings("unchecked")
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> Put writeValueAsPut(T record) {
        validateHBClass((Class<T>) record.getClass());
        return writeValueAsPut0(record);
    }

    <R extends Serializable & Comparable<R>, T extends HBRecord<R>> Put writeValueAsPut0(T record) {
        Put put = new Put(composeRowKey(record));
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> fe : convertRecordToMap(record).entrySet()) {
            byte[] family = fe.getKey();
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> e : fe.getValue().entrySet()) {
                byte[] columnName = e.getKey();
                NavigableMap<Long, byte[]> columnValuesVersioned = e.getValue();
                if (columnValuesVersioned == null)
                    continue;
                for (Map.Entry<Long, byte[]> versionAndValue : columnValuesVersioned.entrySet()) {
                    put.addColumn(family, columnName, versionAndValue.getKey(), versionAndValue.getValue());
                }
            }
        }
        return put;
    }

    /**
     * A <i>bulk version</i> of {@link #writeValueAsPut(HBRecord)} method
     *
     * @param records List of objects of your bean-like class (of type that extends {@link HBRecord})
     * @param <R>     Data type of row key
     * @param <T>     Entity type
     * @return List of HBase's {@link Put} objects
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> List<Put> writeValueAsPut(List<T> records) {
        List<Put> puts = new ArrayList<>(records.size());
        for (T record : records) {
            Put put = writeValueAsPut(record);
            puts.add(put);
        }
        return puts;
    }

    /**
     * <p>Converts an object of your bean-like class to HBase's {@link Result} object.
     * <p>This method is for use in unit-tests of a MapReduce job whose <code>Mapper</code> class extends <code>org.apache.hadoop.hbase.mapreduce.TableMapper</code> class (in other words, a MapReduce job whose input in an HBase table)
     *
     * @param record object of your bean-like class (of type that extends {@link HBRecord})
     * @param <R>    Data type of row key
     * @param <T>    Entity type
     * @return HBase's {@link Result} object
     */
    @SuppressWarnings("unchecked")
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> Result writeValueAsResult(T record) {
        validateHBClass((Class<T>) record.getClass());
        byte[] row = composeRowKey(record);
        List<Cell> cellList = new ArrayList<>();
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> fe : convertRecordToMap(record).entrySet()) {
            byte[] family = fe.getKey();
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> e : fe.getValue().entrySet()) {
                byte[] columnName = e.getKey();
                NavigableMap<Long, byte[]> valuesVersioned = e.getValue();
                if (valuesVersioned == null) {
                    continue;
                }
                for (Map.Entry<Long, byte[]> columnVersion : valuesVersioned.entrySet()) {
                    CellBuilder cellBuilder = CellBuilderFactory.create(CellBuilderType.DEEP_COPY);
                    cellBuilder.setType(Cell.Type.Put).setRow(row).setFamily(family).setQualifier(columnName).setTimestamp(columnVersion.getKey()).setValue(columnVersion.getValue());
                    Cell cell = cellBuilder.build();
                    cellList.add(cell);
                }
            }
        }
        return Result.create(cellList);
    }

    /**
     * A <i>bulk version</i> of {@link #writeValueAsResult(HBRecord)} method
     *
     * @param records List of objects of your bean-like class (of type that extends {@link HBRecord})
     * @param <R>     Data type of row key
     * @param <T>     Entity type
     * @return List of HBase's {@link Result} objects
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> List<Result> writeValueAsResult(List<T> records) {
        List<Result> results = new ArrayList<>(records.size());
        for (T record : records) {
            Result result = writeValueAsResult(record);
            results.add(result);
        }
        return results;
    }

    /**
     * <p>Converts HBase's {@link Result} object to an object of your bean-like class.
     * <p>This method is for use in a MapReduce job whose <code>Mapper</code> class extends <code>org.apache.hadoop.hbase.mapreduce.TableMapper</code> class (in other words, a MapReduce job whose input is an HBase table)
     *
     * @param rowKey Row key of the record that corresponds to {@link Result}. If this is <code>null</code>, an attempt will be made to resolve it from {@link Result}
     * @param result HBase's {@link Result} object
     * @param clazz  {@link Class} to which you want to convert to (must implement {@link HBRecord} interface)
     * @param <R>    Data type of row key
     * @param <T>    Entity type
     * @return Object of bean-like class
     * @throws CodecException One or more column values is a <code>byte[]</code> that couldn't be deserialized into field type (as defined in your entity class)
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValue(ImmutableBytesWritable rowKey, Result result, Class<T> clazz) {
        validateHBClass(clazz);
        if (rowKey == null)
            return readValueFromResult(result, clazz);
        else
            return readValueFromRowAndResult(rowKey.get(), result, clazz);
    }

    /**
     * A compact version of {@link #readValue(ImmutableBytesWritable, Result, Class)} method
     *
     * @param result HBase's {@link Result} object
     * @param clazz  {@link Class} to which you want to convert to (must implement {@link HBRecord} interface)
     * @param <R>    Data type of row key
     * @param <T>    Entity type
     * @return Object of bean-like class
     * @throws CodecException One or more column values is a <code>byte[]</code> that couldn't be deserialized into field type (as defined in your entity class)
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValue(Result result, Class<T> clazz) {
        validateHBClass(clazz);
        return readValueFromResult(result, clazz);
    }

    private boolean isResultEmpty(Result result) {
        if (result == null || result.isEmpty()) return true;
        byte[] rowBytes = result.getRow();
        return rowBytes == null || rowBytes.length == 0;
    }

    <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValueFromResult(Result result, Class<T> clazz) {
        if (isResultEmpty(result)) return null;
        return convertMapToRecord(result.getRow(), result.getMap(), clazz);
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValueFromRowAndResult(byte[] rowKeyBytes, Result result, Class<T> clazz) {
        if (isResultEmpty(result)) {
            return null;
        }
        return convertMapToRecord(rowKeyBytes, result.getMap(), clazz);
    }

    private void objectSetFieldValue(Object obj, Field field, NavigableMap<Long, byte[]> columnValuesVersioned, Map<String, String> codecFlags) {
        if (columnValuesVersioned == null)
            return;
        try {
            field.setAccessible(true);
            NavigableMap<Long, Object> columnValuesVersionedBoxed = new TreeMap<>();
            for (Map.Entry<Long, byte[]> versionAndValue : columnValuesVersioned.entrySet()) {
                columnValuesVersionedBoxed.put(versionAndValue.getKey(), byteArrayToValue(versionAndValue.getValue(), ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[1], codecFlags));
            }
            field.set(obj, columnValuesVersionedBoxed);
        } catch (Exception ex) {
            throw new ConversionFailedException(String.format("Could not set value on field \"%s\" on instance of class %s", field.getName(), obj.getClass()), ex);
        }
    }

    private void objectSetFieldValue(Object obj, Field field, Object value) {
        try {
            field.setAccessible(true);
            field.set(obj, value);
        } catch (IllegalAccessException e) {
            throw new ConversionFailedException(String.format("Could not set value on field \"%s\" on instance of class %s", field.getName(), obj.getClass()), e);
        }
    }

    private void objectSetFieldValue(Object obj, Field field, byte[] value, Map<String, String> codecFlags) {
        if (value == null || value.length == 0)
            return;
        try {
            field.setAccessible(true);
            field.set(obj, byteArrayToValue(value, field.getGenericType(), codecFlags));
        } catch (IllegalAccessException e) {
            throw new ConversionFailedException(String.format("Could not set value on field \"%s\" on instance of class %s", field.getName(), obj.getClass()), e);
        }
    }


    /**
     * Converts a byte array representing HBase column data to appropriate data type (boxed as object)
     *
     * @see #valueToByteArray(Serializable, Map)
     */
    Object byteArrayToValue(byte[] value, Type type, Map<String, String> codecFlags) {
        try {
            if (value == null || value.length == 0)
                return null;
            else
                return codec.deserialize(value, type, codecFlags);
        } catch (DeserializationException e) {
            throw new CodecException("Error while deserializing", e);
        }
    }

    /**
     * <p>Converts HBase's {@link Put} object to an object of your bean-like class
     * <p>This method is for use in unit-tests of a MapReduce job whose <code>Reducer</code> class extends <code>org.apache.hadoop.hbase.mapreduce.TableReducer</code> class (in other words, a MapReduce job whose output is an HBase table)
     *
     * @param rowKey Row key of the record that corresponds to {@link Put}. If this is <code>null</code>, an attempt will be made to resolve it from {@link Put} object
     * @param put    HBase's {@link Put} object
     * @param clazz  {@link Class} to which you want to convert to (must implement {@link HBRecord} interface)
     * @param <R>    Data type of row key
     * @param <T>    Entity type
     * @return Object of bean-like class
     * @throws CodecException One or more column values is a <code>byte[]</code> that couldn't be deserialized into field type (as defined in your entity class)
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValue(ImmutableBytesWritable rowKey, Put put, Class<T> clazz) {
        validateHBClass(clazz);
        if (rowKey == null)
            return readValueFromPut(put, clazz);
        else
            return readValueFromRowAndPut(rowKey.get(), put, clazz);
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValueFromRowAndPut(byte[] rowKeyBytes, Put put, Class<T> clazz) {
        Map<byte[], List<Cell>> rawMap = put.getFamilyCellMap();
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        for (Map.Entry<byte[], List<Cell>> familyNameAndColumnValues : rawMap.entrySet()) {
            byte[] family = familyNameAndColumnValues.getKey();
            map.computeIfAbsent(family, f -> new TreeMap<>(Bytes.BYTES_COMPARATOR));
            List<Cell> cellList = familyNameAndColumnValues.getValue();
            for (Cell cell : cellList) {
                byte[] column = CellUtil.cloneQualifier(cell);
                if (!map.get(family).containsKey(column)) {
                    map.get(family).put(column, new TreeMap<>());
                }
                map.get(family).get(column).put(cell.getTimestamp(), CellUtil.cloneValue(cell));
            }
        }
        return convertMapToRecord(rowKeyBytes, map, clazz);
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValueFromPut(Put put, Class<T> clazz) {
        if (put == null || put.isEmpty() || put.getRow() == null || put.getRow().length == 0) {
            return null;
        }
        return readValueFromRowAndPut(put.getRow(), put, clazz);
    }

    /**
     * A compact version of {@link #readValue(ImmutableBytesWritable, Put, Class)} method
     *
     * @param put   HBase's {@link Put} object
     * @param clazz {@link Class} to which you want to convert to (must implement {@link HBRecord} interface)
     * @param <R>   Data type of row key
     * @param <T>   Entity type
     * @return Object of bean-like class
     * @throws CodecException One or more column values is a <code>byte[]</code> that couldn't be deserialized into field type (as defined in your entity class)
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValue(Put put, Class<T> clazz) {
        validateHBClass(clazz);
        return readValueFromPut(put, clazz);
    }

    /**
     * Get row key (for use in HBase) from a bean-line object.<br>
     * For use in:
     * <ul>
     * <li>reducer jobs that extend HBase's <code>org.apache.hadoop.hbase.mapreduce.TableReducer</code> class</li>
     * <li>unit tests for mapper jobs that extend HBase's <code>org.apache.hadoop.hbase.mapreduce.TableMapper</code> class</li>
     * </ul>
     *
     * @param record object of your bean-like class (of type that extends {@link HBRecord})
     * @param <R>    Data type of row key
     * @param <T>    Entity type
     * @return Serialised row key wrapped in {@link ImmutableBytesWritable}
     * @see #toIbw(Serializable)
     */
    @SuppressWarnings("unchecked")
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> ImmutableBytesWritable getRowKey(T record) {
        if (record == null) {
            throw new NullPointerException("Cannot compose row key for null objects");
        }
        validateHBClass((Class<T>) record.getClass());
        return new ImmutableBytesWritable(composeRowKey(record));
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> byte[] composeRowKey(T record) {
        R rowKey;
        try {
            rowKey = record.composeRowKey();
        } catch (Exception ex) {
            throw new RowKeyCantBeComposedException(ex);
        }
        if (rowKey == null || rowKey.toString().isEmpty()) {
            throw new RowKeyCantBeEmptyException();
        }
        @SuppressWarnings("unchecked")
        WrappedHBTable<R, T> hbTable = new WrappedHBTable<>((Class<T>) record.getClass());
        return valueToByteArray(rowKey, hbTable.getCodecFlags());
    }

    /**
     * Get list of column families and their max versions, mapped in definition of your bean-like class
     *
     * @param clazz {@link Class} that you're reading (must implement {@link HBRecord} interface)
     * @param <R>   Data type of row key
     * @param <T>   Entity type
     * @return Map of column families and their max versions
     */
    <R extends Serializable & Comparable<R>, T extends HBRecord<R>> Map<String, Integer> getColumnFamiliesAndVersions(Class<T> clazz) {
        final WrappedHBTable<R, T> hbTable = validateHBClass(clazz);
        return hbTable.getFamiliesAndVersions();
    }


    /**
     * Checks whether input class can be converted to HBase data types and vice-versa
     *
     * @param clazz {@link Class} you intend to validate (must implement {@link HBRecord} interface)
     * @param <R>   Data type of row key
     * @param <T>   Entity type
     * @return <code>true</code> or <code>false</code>
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> boolean isValid(Class<T> clazz) {
        try {
            validateHBClass(clazz);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    /**
     * For your bean-like {@link Class}, get all fields mapped to HBase columns
     *
     * @param clazz Bean-like {@link Class} (must implement {@link HBRecord} interface) whose fields you intend to read
     * @param <R>   Data type of row key
     * @param <T>   Entity type
     * @return A {@link Map} with keys as field names and values as instances of {@link Field}
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> Map<String, Field> getHBColumnFields(Class<T> clazz) {
        validateHBClass(clazz);
        return getHBColumnFields0(clazz);
    }

    <R extends Serializable & Comparable<R>, T extends HBRecord<R>> Map<String, Field> getHBColumnFields0(Class<T> clazz) {
        Map<String, Field> mappings = new LinkedHashMap<>();
        Class<?> thisClass = clazz;
        while (thisClass != null && thisClass != Object.class) {
            for (Field field : thisClass.getDeclaredFields()) {
                if (new WrappedHBColumn(field).isPresent()) {
                    mappings.put(field.getName(), field);
                }
            }
            Class<?> parentClass = thisClass.getSuperclass();
            thisClass = parentClass.isAnnotationPresent(MappedSuperClass.class) ? parentClass : null;
        }
        return mappings;
    }
}