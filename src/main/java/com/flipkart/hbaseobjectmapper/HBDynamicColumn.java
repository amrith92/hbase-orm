package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.codec.Codec;

import java.io.Serializable;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.Type;
import java.util.Map;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Maps an entity field to a dynamic HBase column.
 */
@Documented
@Target(FIELD)
@Retention(RUNTIME)
public @interface HBDynamicColumn {

    /**
     * Required: Name of HBase column family
     *
     * @return Name of HBase column family
     */
    String family();

    /**
     * Optional: Name of the field which values will be use for the  column.
     *
     * @return Name of field which value will be used for the column name
     */
    DynamicQualifier qualifier();

    /**
     * A column prefix.
     *
     * @return prefix of the resulting column name
     */
    String prefix() default "def";

    /**
     * Optional separator between the prefix and the qualifier field value.
     *
     * @return separator
     */
    String separator() default "#";

    /**
     * Optional parameter that determines whether to preserve order or not.
     *
     * @return true when order should be preserved
     */
    boolean preserveOrder() default false;

    /**
     * <b>[optional]</b> flags to be passed to codec's {@link Codec#serialize(Serializable, Map) serialize} and {@link Codec#deserialize(byte[], Type, Map) deserialize} methods
     * <p>
     * Note: These flags will be passed as a <code>Map&lt;String, String&gt;</code> (param name and param value)
     *
     * @return Flags
     */
    Flag[] codecFlags() default {};
}
