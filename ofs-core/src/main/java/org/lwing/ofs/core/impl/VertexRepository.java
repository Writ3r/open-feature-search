/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl;

import java.util.UUID;
import org.lwing.ofs.core.api.config.OFSConfiguration;
import org.lwing.ofs.core.api.VertexType;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.impl.lock.CloseableResourceLock;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.lwing.ofs.core.api.exception.VertexNotFoundException;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Lucas Wing
 */
public abstract class VertexRepository extends OFSRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(VertexRepository.class);

    protected final VertexType vertexType;

    protected VertexRepository(JanusGraph graph, OFSConfiguration config, VertexType vertexType) {
        super(graph, config);
        this.vertexType = vertexType;
    }

    /**
     * Verifies a vertex is not being referenced by any in-edges before we allow
     * for a mutation of that vertex
     *
     * @param g traversal source to use for checking in-edges
     * @param vertexId id if the vertex to check
     * @throws GraphIntegrityException thrown if there is an in-edge to the
     * specified vertex
     */
    public static void verifyIntegretyBeforeMutation(GraphTraversalSource g, String vertexId) throws GraphIntegrityException {
        Long inEdgeCount = getInEdgeCount(g, vertexId);
        if (inEdgeCount > 0) {
            LOGGER.debug("In edge count of node id {} is {}", vertexId, inEdgeCount);
            throw new GraphIntegrityException("Failed to validate node with id [%s] has input edges and therefore is still immutable.", vertexId);
        }
    }

    private static Long getInEdgeCount(GraphTraversalSource g, String vertexId) {
        GraphTraversal<Vertex, Long> traversal = g.V().hasId(vertexId).inE().count();
        return traversal.next();
    }

    protected void deleteVertex(String id, OFSType type) throws GraphIntegrityException, Exception {
        LOGGER.debug("Deleting node id: {} from the Graph", id);
        try ( GraphTraversalSource g = getTraversalSource()) {
            try ( Transaction tx = g.tx()) {
                try ( CloseableResourceLock lock = acquireLock(type, id)) {
                    verifyIntegretyBeforeMutation(g, id);
                    g.V(id).outE().drop().iterate();
                    g.V(id).drop().iterate();
                    tx.commit();
                }
            }
        }
    }

    protected void deleteVertex(String id, GraphTraversalSource g) throws GraphIntegrityException, Exception {
        LOGGER.debug("Deleting node id: {} from the Graph", id);
        verifyIntegretyBeforeMutation(g, id);
        g.V(id).outE().drop().iterate();
        g.V(id).drop().iterate();
    }

    /**
     * Checks if a vertex meets the criteria of being a certain VertexType
     *
     * @see org.lwing.ofs.core.api.VertexType
     *
     * @param g traversal source to use for the check
     * @param vertexId vertex id to check
     * @param vertexType required vertex type
     * @throws GraphIntegrityException if the vertex does not meet the required
     * type
     */
    protected void verifyVertexIsType(GraphTraversalSource g, String vertexId, VertexType vertexType) throws GraphIntegrityException {
        if (!g.V(vertexId).has(OFSRepository.NODE_TYPE_FIELD, vertexType.name()).hasNext()) {
            throw new GraphIntegrityException("Input vertex [%s] is not of the expected type [%s]", vertexId, vertexType.name());
        }
    }

    /**
     * Checks if a vertex meets the criteria of being a certain VertexType
     *
     * @param v vertex to check
     * @see org.lwing.ofs.core.api.VertexType
     * @param vertexType required vertex type
     * @throws GraphIntegrityException if the vertex does not meet the required
     * type
     */
    protected void verifyVertexIsType(Vertex v, VertexType vertexType) throws GraphIntegrityException {
        if (!v.property(OFSRepository.NODE_TYPE_FIELD).value().equals(vertexType.name())) {
            throw new GraphIntegrityException("Input vertex [%s] is not of the expected type [%s]", v.id(), vertexType.name());
        }
    }

    /**
     * Checks if a vertex meets the criteria of being a certain VertexType. This
     * vertex type is the one specified on the VertexRepository.
     *
     * @param v vertex to check
     * @see org.lwing.ofs.core.api.VertexType
     * @throws GraphIntegrityException if the vertex does not meet the required
     * type
     */
    protected void verifyVertexIsType(Vertex v) throws GraphIntegrityException {
        verifyVertexIsType(v, vertexType);
    }

    /**
     * Reads a vertex from the graph and verify it fits the repository type
     *
     * @param id identifier of the node
     * @param g graph traversal source
     * @return
     * @throws VertexNotFoundException
     * @throws GraphIntegrityException
     */
    protected Vertex readVertexFromGraph(String id, GraphTraversalSource g) 
            throws VertexNotFoundException, GraphIntegrityException {
        GraphTraversal<Vertex, Vertex> ver = g.V(id);
        if (!ver.hasNext()) {
            throw new VertexNotFoundException(id);
        }
        Vertex vertex = ver.next();
        verifyVertexIsType(vertex);
        return vertex;
    }

    /**
     * Creates a vertex with the default parameters on it
     *
     * @param source the traversal source used to create the vertex
     * @param id node id
     * @return the added vertex's traversal
     */
    protected GraphTraversal<Vertex, Vertex> addVertex(GraphTraversalSource source, String id) {
        GraphTraversal<Vertex, Vertex> vertex = source.addV();
        // add id
        if (id == null) {
            id = UUID.randomUUID().toString().replace("-", "");
        }
        vertex.property(T.id, id);
        // add node type property
        vertex.property(OFSRepository.NODE_TYPE_FIELD, vertexType.name());
        // return
        return vertex;
    }

    /**
     * Quick method to check if a vertex exists in the graph or not
     *
     * @param id identifier to lookup
     * @return if the vertex exists or not
     * @throws Exception
     */
    public boolean vertexExists(String id) throws Exception {
        try ( GraphTraversalSource g = getTraversalSource()) {
            return g.V(id).hasNext();
        }
    }

}
