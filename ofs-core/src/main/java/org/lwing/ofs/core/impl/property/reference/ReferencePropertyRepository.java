/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.property.reference;

import org.lwing.ofs.core.api.JGMgntProvider;
import org.lwing.ofs.core.api.config.OFSConfiguration;
import static org.lwing.ofs.core.api.config.OFSConfiguration.MODEL_ID;
import org.lwing.ofs.core.api.VertexType;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.exception.InternalKeywordException;
import org.lwing.ofs.core.api.property.RefPropertyKey;
import org.lwing.ofs.core.impl.OFSRepository;
import org.lwing.ofs.core.impl.TypeVerifier;
import org.lwing.ofs.core.impl.lock.CloseableResourceLock;
import org.lwing.ofs.core.impl.property.AbsPropertyRepository;
import org.lwing.ofs.core.impl.property.PropertyRepository;
import static org.lwing.ofs.core.impl.property.PropertyRepository.ALLOWABLE_REF_FIELD;
import static org.lwing.ofs.core.impl.property.PropertyRepository.PROPERTY_NAME;
import static org.lwing.ofs.core.impl.property.PropertyRepository.REF_CARDINALITY;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.V;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphManagement;

/**
 *
 * @author Lucas Wing
 */
public class ReferencePropertyRepository extends AbsPropertyRepository {

    public ReferencePropertyRepository(JanusGraph graph, OFSConfiguration config) {
        super(graph, VertexType.REF_PROPERTY, config);
    }

    /**
     * Creates a property definition which allows for creating references to
     * features which inherit from the stated Model object.
     *
     * @param property reference property to create
     * @throws InternalKeywordException if the property being created is
     * considered internal
     * @throws GraphIntegrityException if the vertex id being passed in for the
     * model is not a model
     * @throws Exception generic JanusGraph exception
     */
    public void createProperty(RefPropertyKey property) throws InternalKeywordException, GraphIntegrityException, Exception {
        if (!config.isAllowInternalFieldActions()) {
            verifyNotReservedField(property.getName());
        }
        try ( CloseableResourceLock lock = acquireLock(property)) {
            // rectify if needed
            rectifyPropertyIfNeeded(property.getName());
            // create the ref property record
            try ( GraphTraversalSource g = getTraversalSource()) {
                try ( Transaction tx = g.tx()) {
                    verifyVertexIsType(g, property.getModelId(), VertexType.MODEL);
                    String refNodeId = (String) addVertex(g, property.getId()).next().id();
                    g.V(refNodeId).property(MODEL_ID, property.getModelId()).next();
                    g.V(refNodeId).property(PROPERTY_NAME, property.getName()).next();
                    g.V(refNodeId).property(REF_CARDINALITY, property.getCardinality().name()).next();
                    for (Object allowableVal : property.getAllowableValues()) {
                        TypeVerifier.verifyFeatureFitsProp((String) allowableVal, g, property.getModelId());
                        g.V(refNodeId).addE(ALLOWABLE_REF_FIELD).to(V(allowableVal)).next();
                    }
                    g.V(refNodeId).addE(OFSConfiguration.REF_USES_MODEL).to(V(property.getModelId())).next();
                    tx.commit();
                }
            }
            // if the above transaction worked, then create an actual property
            // this is used so SELECT statements can run for queries on ref properties without them failing on the schema
            // not allowing for the property to be searched for
            try ( JGMgntProvider mgnt = useManagement()) {
                JanusGraphManagement management = mgnt.getMgnt();
                PropertyKey key = management.makePropertyKey(property.getName())
                        .dataType(String.class).cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
                management.setConsistency(key, ConsistencyModifier.LOCK);
            }
        }
    }

    /**
     * Retrieve a list of all reference properties in the system
     *
     * @return list of all reference properties
     * @throws Exception generic JanusGraph exception
     */
    public List<RefPropertyKey> listRefProps() throws Exception {
        List<RefPropertyKey> outProps = new ArrayList<>();
        try ( GraphTraversalSource g = getTraversalSource()) {
            try ( JGMgntProvider mgnt = useManagement()) {
                getVertices(g).forEachRemaining(v -> {
                    String propName = (String) v.property(PropertyRepository.PROPERTY_NAME).value();
                    Optional<RefPropertyKey> readKey = readRefProperty(propName, mgnt.getMgnt(), v);
                    if (readKey.isPresent()) {
                        outProps.add(readKey.get());
                    }
                });
            }
        }
        return outProps;
    }

    /**
     * Reads a reference property & returns a populated optional if it exists.
     * An empty optional is returned if it does not.
     *
     * @param name property to lookup the object for
     * @return Optional of the looked up object
     * @throws Exception generic JanusGraph exception
     */
    public Optional<RefPropertyKey> readRefProperty(String name) throws Exception {
        try ( GraphTraversalSource g = getTraversalSource()) {

            GraphTraversal<Vertex, Vertex> search = getVertex(name, g);
            if (!search.hasNext()) {
                return Optional.empty();
            }
            
            Vertex vertex = search.next();
            verifyVertexIsType(vertex);
            try ( JGMgntProvider mgnt = useManagement()) {
                return readRefProperty(name, mgnt.getMgnt(), vertex);
            }
        }
    }

    private Optional<RefPropertyKey> readRefProperty(String name, JanusGraphManagement management, Vertex v) {
        // ensure prop exists in the schema
        PropertyKey out = management.getPropertyKey(name);
        if (out == null) {
            return Optional.empty();
        }
        return Optional.of(new RefRepoProperty(v));
    }

    private GraphTraversal<Vertex, Vertex> getVertex(String name, GraphTraversalSource g) {
        return getVertices(g).has(PROPERTY_NAME, name);
    }

    private GraphTraversal<Vertex, Vertex> getVertices(GraphTraversalSource g) {
        return g.V().has(OFSRepository.NODE_TYPE_FIELD, VertexType.REF_PROPERTY.name());
    }

}
