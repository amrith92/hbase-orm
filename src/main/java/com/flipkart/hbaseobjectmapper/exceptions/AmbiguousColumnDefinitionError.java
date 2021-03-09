package com.flipkart.hbaseobjectmapper.exceptions;

import javax.annotation.Nonnull;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.stream.Collectors;

public class AmbiguousColumnDefinitionError extends IllegalArgumentException {

    public AmbiguousColumnDefinitionError(@Nonnull Field field, @Nonnull Annotation... ambiguousAnnotations) {
        super(String.format("Class %s has a field %s that's annotated with the annotations %s (you can use only one of them on a field)", field.getDeclaringClass(), field.getName(), stringifyAnnotations(ambiguousAnnotations)));
    }

    private static String stringifyAnnotations(final Annotation[] ambiguousAnnotations) {
        return Arrays.stream(ambiguousAnnotations).map(annotation -> annotation.annotationType().getName()).collect(Collectors.joining(", "));
    }
}
