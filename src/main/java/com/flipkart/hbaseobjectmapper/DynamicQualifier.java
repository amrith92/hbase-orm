package com.flipkart.hbaseobjectmapper;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Represents a dynamic qualifier.
 */
@Documented
@Target(TYPE)
@Retention(RUNTIME)
public @interface DynamicQualifier {

    /**
     * The fields that constitute the qualifier.
     *
     * @return field names
     */
    String[] parts();

    /**
     * A separator between the field values.
     *
     * @return non-blank separator
     */
    String separator() default ":";
}
