/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.index;

import com.google.common.collect.Iterators;
import java.io.IOException;
import org.lwing.ofs.core.api.JGMgntProvider;
import org.lwing.ofs.core.api.config.OFSConfiguration;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.exception.InternalKeywordException;
import org.lwing.ofs.core.api.index.IndexType;
import org.lwing.ofs.core.api.index.Index;
import org.lwing.ofs.core.impl.OFSRepository;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.JanusGraphManagement.IndexBuilder;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.lwing.ofs.core.impl.lock.CloseableResourceLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Lucas Wing
 */
public class IndexRepository extends OFSRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexRepository.class);

    public IndexRepository(JanusGraph graph, OFSConfiguration config) {
        super(graph, config);
    }

    /**
     * Creates a new index via JanusGraph NOTE: Never create new indexes while a
     * transaction is active.
     * https://docs.janusgraph.org/schema/index-management/index-performance/#:~:text=A%20vertex-centric%20index%20is,unique%20name%20for%20the%20index.
     * https://stackoverflow.com/questions/76266499/register-index-not-registering-schemastatus-installed-janusgraph-index
     *
     * @param index object to create via JanusGraphManagement
     * @param waitForRegister if the thread should wait until the index
     * registers or not before moving forward
     * @return newly created Index object
     * @throws GraphIntegrityException if we can't find the fields associated
     * with the index
     * @throws InterruptedException if the thread is interrupted while waiting
     * for the index to register
     * @throws ExecutionException generic JanusGraph exception
     * @throws InternalKeywordException if you attempt to create an internal
     * keyword for an index name
     */
    public Index createIndex(Index index, boolean waitForRegister) throws GraphIntegrityException, InterruptedException, ExecutionException, InternalKeywordException, IOException {
        graph.tx().rollback();
        try ( JGMgntProvider management = useManagement()) {
            JanusGraphManagement mgmt = management.getMgnt();
            createIndex(index, mgmt);
            mgmt.commit();
        }
        if (waitForRegister) {
            LOGGER.info("Waiting for index [{}] to register. This may take awhile.", index.getName());
            ManagementSystem.awaitGraphIndexStatus(graph, index.getName()).status(SchemaStatus.REGISTERED).call();
        }
        return readIndex(index.getName());
    }

    /**
     * Internal method for creating an index, use the other createIndex method.
     *
     * @param index object to create in JanusGraph management
     * @param mgmt JanusGraph management object
     * @throws GraphIntegrityException if we can't find the fields associated
     * with the index
     * @throws InternalKeywordException if you attempt to create an internal
     * keyword for an index name
     */
    public void createIndex(Index index, JanusGraphManagement mgmt) throws GraphIntegrityException, InternalKeywordException, IOException {
        if (!config.isAllowInternalFieldActions()) {
            verifyNotReservedField(index.getName());
        }
        try ( CloseableResourceLock lock = acquireLock(index)) {
            IndexBuilder ib = mgmt.buildIndex(index.getName(), index.getFieldType().getElementType());
            for (Entry<String, Parameter[]> entry : index.getProperties().entrySet()) {
                PropertyKey key = mgmt.getPropertyKey(entry.getKey());
                if (key == null) {
                    throw new GraphIntegrityException("Failed to find key associated with property [%s]", entry.getKey());
                }
                ib.addKey(key, entry.getValue());
            }
            if (index.isUnique()) {
                ib.unique();
            }
            if (index.getIndexType().equals(IndexType.COMPOSITE)) {
                ib.buildCompositeIndex();
            } else {
                ib.buildMixedIndex(config.getMixedIndexName());
            }
        }
    }

    /**
     * Reads an index object using the specified name
     *
     * @param indexName name of index to lookup
     * @return Index object looked up from JanusGraph Management
     */
    public Index readIndex(String indexName) {
        try ( JGMgntProvider management = useManagement()) {
            return new RepoIndex(management.getMgnt().getGraphIndex(indexName));
        }
    }

    /**
     * Checks if the graph contains the specified index
     *
     * @param indexName name of index to lookup
     * @return Index object looked up from JanusGraph Management
     */
    public boolean containsIndex(String indexName) {
        try ( JGMgntProvider management = useManagement()) {
            return management.getMgnt().containsGraphIndex(indexName);
        }
    }

    /**
     * This method is a simple way to delete indexes ignoring any sort of state
     * error handling if a schema status were to fail to change
     *
     * @see
     * <a href="https://docs.janusgraph.org/schema/index-management/index-removal/">JanusGraph
     * docs</a>
     * @param indexName name of the index to delete
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws InternalKeywordException if the index name is considered an
     * internal index
     */
    public void removeIndex(String indexName) throws InterruptedException, ExecutionException, InternalKeywordException {
        if (!config.isAllowInternalFieldActions()) {
            verifyNotReservedField(indexName);
        }
        // disable
        updateIndex(indexName, SchemaAction.DISABLE_INDEX);
        awaitIndexStatus(indexName, SchemaStatus.DISABLED);
        // discard
        updateIndex(indexName, SchemaAction.DISCARD_INDEX);
        awaitIndexStatus(indexName, SchemaStatus.DISCARDED);
        // remove entirely
        updateIndex(indexName, SchemaAction.DROP_INDEX);
    }

    /**
     * Lists all indices
     *
     * @return set of all indices in JanusGraph management
     */
    public Set<Index> listIndices() {
        try ( JGMgntProvider management = useManagement()) {
            JanusGraphManagement mgmt = management.getMgnt();
            Iterator<JanusGraphIndex> verIterator = mgmt.getGraphIndexes(Vertex.class).iterator();
            Iterator<JanusGraphIndex> edgeIterator = mgmt.getGraphIndexes(Edge.class).iterator();
            Iterator<JanusGraphIndex> combined = Iterators.concat(verIterator, edgeIterator);
            Set<Index> out = new HashSet<>();
            Iterators.transform(combined, RepoIndex::new).forEachRemaining(out::add);
            return out;
        }
    }

    /**
     * Updates an Index via the specified Schema Action
     *
     * @param name index to update
     * @param action action to apply
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws InternalKeywordException if the name is considered an internal
     * index
     */
    public void updateIndex(String name, SchemaAction action) throws InterruptedException, ExecutionException, InternalKeywordException {
        if (!config.isAllowInternalFieldActions()) {
            verifyNotReservedField(name);
        }
        try ( JGMgntProvider management = useManagement()) {
            JanusGraphManagement mgmt = management.getMgnt();
            mgmt.updateIndex(mgmt.getGraphIndex(name), action).get();
        }
    }

    /**
     * Waits for an index to hit a certain specified status
     *
     * @param indexName name of the index to look at for the stated status
     * @param status
     * @throws InterruptedException
     */
    public void awaitIndexStatus(String indexName, SchemaStatus status) throws InterruptedException {
        ManagementSystem.awaitGraphIndexStatus(graph, indexName).status(status).call();
    }

}
