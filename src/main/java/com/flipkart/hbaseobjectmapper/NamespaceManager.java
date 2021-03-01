package com.flipkart.hbaseobjectmapper;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * Manages DAOs for record types under distinct namespaces. This is useful to have
 * when the namespaces are not known upfront.
 */
@SuppressWarnings("unchecked")
public class NamespaceManager {

    private final ConcurrentMap<String, ConcurrentMap<Class<? extends HBRecord<?>>, AbstractHBDAO<?, ?>>>
        synchronousDaoCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<Class<? extends HBRecord<?>>, Supplier<AbstractHBDAO<?, ?>>> synchronousDaoCreators = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentMap<Class<? extends HBRecord<?>>, ReactiveHBDAO<?, ?>>>
            asynchronousDaoCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<Class<? extends HBRecord<?>>, Supplier<ReactiveHBDAO<?, ?>>> asynchronousDaoCreators = new ConcurrentHashMap<>();

    /**
     * Configure an asynchronous DAO creator for a given record type.
     *
     * @param recordType a record type
     * @param daoCreator a DAO creator
     * @return fluent interface
     */
    public NamespaceManager configureAsyncDao(@Nonnull final Class<? extends HBRecord<?>> recordType, @Nonnull final Supplier<ReactiveHBDAO<?, ?>> daoCreator) {
        asynchronousDaoCreators.putIfAbsent(recordType, daoCreator);
        return this;
    }

    /**
     * Configure a DAO creator for a given record type.
     *
     * @param recordType a record type
     * @param daoCreator a DAO creator
     * @return fluent interface
     */
    public NamespaceManager configureDao(@Nonnull final Class<? extends HBRecord<?>> recordType, @Nonnull final Supplier<AbstractHBDAO<?, ?>> daoCreator) {
        synchronousDaoCreators.putIfAbsent(recordType, daoCreator);
        return this;
    }

    /**
     * Retrieves an async DAO instance.
     *
     * @param aNamespace a namespace name
     * @param aRecordType a record type
     * @param <R> Row key type
     * @param <T> Record type
     * @return reactive DAO instance
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> ReactiveHBDAO<R, T> getAsyncDaoFor(@Nonnull final String aNamespace, @Nonnull final Class<? extends HBRecord<?>> aRecordType) {
        return (ReactiveHBDAO<R, T>) asynchronousDaoCache.computeIfAbsent(aNamespace, discard -> new ConcurrentHashMap<>())
                .computeIfAbsent(aRecordType, discard -> {
                    final Supplier<? extends ReactiveHBDAO<?, ?>> creator = asynchronousDaoCreators.get(aRecordType);
                    if (creator == null) {
                        throw new IllegalStateException("No creator configured for async dao of record type " + aRecordType);
                    } else {
                        final ReactiveHBDAO<?, ?> reactiveHBDAO = creator.get();
                        reactiveHBDAO.setNamespace(aNamespace);
                        return reactiveHBDAO;
                    }
                });
    }

    /**
     * Retrieves an DAO instance.
     *
     * @param aNamespace a namespace name
     * @param aRecordType a record type
     * @param <R> Row key type
     * @param <T> Record type
     * @return DAO instance
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> AbstractHBDAO<R, T> getDaoFor(@Nonnull final String aNamespace, @Nonnull final Class<? extends HBRecord<?>> aRecordType) {
        return (AbstractHBDAO<R, T>) synchronousDaoCache.computeIfAbsent(aNamespace, discard -> new ConcurrentHashMap<>())
                .computeIfAbsent(aRecordType, discard -> {
                    final Supplier<? extends AbstractHBDAO<?, ?>> creator = synchronousDaoCreators.get(aRecordType);
                    if (creator == null) {
                        throw new IllegalStateException("No creator configured for dao of record type " + aRecordType);
                    } else {
                        final AbstractHBDAO<?, ?> abstractHBDAO = creator.get();
                        abstractHBDAO.setNamespace(aNamespace);
                        return abstractHBDAO;
                    }
                });
    }
}
