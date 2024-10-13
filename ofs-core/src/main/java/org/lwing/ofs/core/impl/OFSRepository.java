/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.lwing.ofs.core.api.JGMgntProvider;
import org.lwing.ofs.core.api.config.OFSConfiguration;
import static org.lwing.ofs.core.api.config.OFSConfiguration.INTERNAL_FIELD_PREFIX;
import org.lwing.ofs.core.api.exception.InternalKeywordException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.lwing.ofs.core.api.state.DependencyResource;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;
import org.lwing.ofs.core.api.state.StatefulResource;
import org.lwing.ofs.core.impl.lock.CloseableResourceLock;

/**
 *
 * @author Lucas Wing
 */
public abstract class OFSRepository {

    // used for telling what type a node is (feature, model, view, etc)
    public static final String NODE_TYPE_FIELD = INTERNAL_FIELD_PREFIX + "node_type";

    protected final OFSConfiguration config;

    protected final JanusGraph graph;

    protected OFSRepository(JanusGraph graph, OFSConfiguration config) {
        this.graph = graph;
        this.config = config;
    }

    /**
     * @return Gremlin traversal source
     */
    protected GraphTraversalSource getTraversalSource() {
        return graph.traversal();
    }

    /**
     * @return Provider of a JanusGraph Management Object
     */
    protected JGMgntProvider useManagement() {
        return new JGMgntProvider(graph.openManagement());
    }
    
    /**
     * Checks if a given field is an internal field or not.
     * 
     * @param field given field to check
     * @throws InternalKeywordException if field is internal
     */
    protected void verifyNotReservedField(String field) throws InternalKeywordException {
        if (checkIfReservedField(field)) {
            throw new InternalKeywordException(field);
        }
    }
    
    protected boolean checkIfReservedField(String field) {
        return field.startsWith(OFSConfiguration.INTERNAL_FIELD_PREFIX);
    }
    
    /**
     * Get lock on resources that are affected by an operation
     *
     * @param type type of node to lock
     * @param nodeIds resources to lock
     * @return closable semaphore for try with statements
     */
    protected CloseableResourceLock acquireLock(OFSType type, String... nodeIds) {
        return getOpenClosableResourceLock(DependencyResource.getNodeResources(type, Arrays.asList(nodeIds)));
    }

    /**
     * Get lock on resource that are affected by an operation
     *
     * @param property resource to lock
     * @return closable semaphore for try with statements
     */
    protected CloseableResourceLock acquirePropertyLock(String property) {
        return getOpenClosableResourceLock(new HashSet<>(Arrays.asList(DependencyResource.fromProperty(OFSType.JPROPERTY, property))));
    }

    /**
     * Get lock on resources that are affected by an operation
     *
     * @param properties resources to lock
     * @return closable semaphore for try with statements
     */
    protected CloseableResourceLock acquirePropertyLock(Set<String> properties) {
        return getOpenClosableResourceLock(DependencyResource.getPropertyResources(OFSType.JPROPERTY, properties));
    }

    /**
     * Get lock on resources that are affected by an operation
     *
     * @param resource resources to lock
     * @return closable semaphore for try with statements
     */
    protected CloseableResourceLock acquireLock(StatefulResource resource) {
        return getOpenClosableResourceLock(resource.calcDependencies());
    }

    private CloseableResourceLock getOpenClosableResourceLock(Set<DependencyResource> resources) {
        return (new CloseableResourceLock(
                config.getResourceLock(),
                DependencyResource.getStringResources(resources)
        )).open();
    }

}
