/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.property;

import org.lwing.ofs.core.api.JGMgntProvider;
import org.lwing.ofs.core.api.config.OFSConfiguration;
import org.lwing.ofs.core.api.VertexType;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.impl.OFSRepository;
import org.lwing.ofs.core.impl.VertexRepository;
import org.lwing.ofs.core.impl.lock.CloseableResourceLock;
import static org.lwing.ofs.core.impl.property.PropertyRepository.PROPERTY_NAME;
import java.util.UUID;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;

/**
 *
 * @author Lucas Wing
 */
public abstract class AbsPropertyRepository extends VertexRepository {

    protected AbsPropertyRepository(JanusGraph graph, VertexType type, OFSConfiguration config) {
        super(graph, config, type);
    }

    protected String deletePropertyRecord(String name) {
        String randomUuid = UUID.randomUUID().toString().replace("-", "");
        String newName = OFSConfiguration.INTERNAL_FIELD_PREFIX + randomUuid;
        try ( JGMgntProvider mgnt = useManagement()) {
            JanusGraphManagement management = mgnt.getMgnt();
            PropertyKey prop = management.getPropertyKey(name);
            management.changeName(prop, newName);
        }
        return newName;
    }

    /**
     * Deletes a stated property.
     *
     * Keep in mind, in JanusGraph deleting Properties isn't a thing, you have
     * to rename them, as per: https://docs.janusgraph.org/schema/ So we just
     * rename the property to a random UUID and return
     *
     * @param name name of the property to delete
     * @return re-named property key (random uuid)
     * @throws GraphIntegrityException if the field is reserved
     * @throws Exception generic JanusGraph exception
     */
    public String deleteProperty(String name) throws GraphIntegrityException, Exception {
        if (!config.isAllowInternalFieldActions()) {
            verifyNotReservedField(name);
        }
        try ( CloseableResourceLock lock = acquirePropertyLock(name)) {
            // delete the vertex
            try ( GraphTraversalSource g = getTraversalSource()) {
                GraphTraversal<Vertex, Vertex> search = getPrimitveOrRefPropertyVertex(name, g);
                if (search.hasNext()) {
                    // ignore the vertex type being locked, it means nothing here
                    deleteVertex((String) search.next().id(), OFSType.FEATURE_SCHEMA);
                }
            }
            // delete the property record
            return deletePropertyRecord(name);
        }
    }

    /**
     * Because nodes and their associated JanusGraph properties are not coupled
     * in the same transaction, it can end up where the node exists or the
     * property exists but the other does not (If some sort of failure occurs)
     *
     * This method exits to deal with OFS props that run into that problem
     *
     * @param property prop to rectify if needed
     * @throws java.lang.Exception generic JanusGraph exception
     */
    protected void rectifyPropertyIfNeeded(String property) throws Exception {
        if (checkIfReservedField(property)) {
            return;
        }
        try ( GraphTraversalSource g = getTraversalSource()) {
            try ( JGMgntProvider mgnt = useManagement()) {
                GraphTraversal<Vertex, Vertex> vertex = getPrimitveOrRefPropertyVertex(property, g);
                boolean nodeExists = vertex.hasNext();
                boolean propExists = mgnt.getMgnt().getPropertyKey(property) != null;
                if (!nodeExists && propExists) {
                    deletePropertyRecord(property);
                } else if (nodeExists && !propExists) {
                    // ignore the vertex type being locked, it means nothing here
                    deleteVertex((String) vertex.next().id(), OFSType.FEATURE_SCHEMA);
                }
            }
        }
    }

    private GraphTraversal<Vertex, Vertex> getPrimitveOrRefPropertyVerticies(GraphTraversalSource g) {
        return g.V().has(OFSRepository.NODE_TYPE_FIELD,
                P.within(VertexType.PRIM_PROPERTY.name(), VertexType.REF_PROPERTY.name()));
    }

    private GraphTraversal<Vertex, Vertex> getPrimitveOrRefPropertyVertex(String name, GraphTraversalSource g) {
        return getPrimitveOrRefPropertyVerticies(g).has(PROPERTY_NAME, name);
    }

}
