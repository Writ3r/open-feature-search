/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.property.primitive;

import org.lwing.ofs.core.api.JGMgntProvider;
import org.lwing.ofs.core.api.config.OFSConfiguration;
import static org.lwing.ofs.core.api.config.OFSConfiguration.INTERNAL_FIELD_PREFIX;
import org.lwing.ofs.core.api.VertexType;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.property.PrimitivePropertyKey;
import org.lwing.ofs.core.impl.OFSRepository;
import org.lwing.ofs.core.impl.property.AbsPropertyRepository;
import org.lwing.ofs.core.impl.property.PropertyRepository;
import static org.lwing.ofs.core.impl.property.PropertyRepository.PROPERTY_NAME;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.lwing.ofs.core.impl.lock.CloseableResourceLock;

/**
 *
 * @author Lucas Wing
 */
public class PrimitivePropertyRepository extends AbsPropertyRepository {

    public static final String ALLOWABLE_STRING_FIELD = INTERNAL_FIELD_PREFIX + "allowable_string_value";
    public static final String ALLOWABLE_CHARACTER_FIELD = INTERNAL_FIELD_PREFIX + "allowable_character_value";
    public static final String ALLOWABLE_BOOLEAN_FIELD = INTERNAL_FIELD_PREFIX + "allowable_bool_value";
    public static final String ALLOWABLE_BYTE_FIELD = INTERNAL_FIELD_PREFIX + "allowable_byte_value";
    public static final String ALLOWABLE_SHORT_FIELD = INTERNAL_FIELD_PREFIX + "allowable_short_value";
    public static final String ALLOWABLE_INTEGER_FIELD = INTERNAL_FIELD_PREFIX + "allowable_integer_value";
    public static final String ALLOWABLE_LONG_FIELD = INTERNAL_FIELD_PREFIX + "allowable_long_value";
    public static final String ALLOWABLE_FLOAT_FIELD = INTERNAL_FIELD_PREFIX + "allowable_float_value";
    public static final String ALLOWABLE_DOUBLE_FIELD = INTERNAL_FIELD_PREFIX + "allowable_double_value";
    public static final String ALLOWABLE_DATE_FIELD = INTERNAL_FIELD_PREFIX + "allowable_date_value";
    public static final String ALLOWABLE_GEOSHAPE_FIELD = INTERNAL_FIELD_PREFIX + "allowable_geoshape_value";
    public static final String ALLOWABLE_UUID_FIELD = INTERNAL_FIELD_PREFIX + "allowable_uuid_value";

    public PrimitivePropertyRepository(JanusGraph graph, OFSConfiguration config) {
        super(graph, VertexType.PRIM_PROPERTY, config);
    }

    /**
     * Creates a primitive property definition which allows for adding primitive
     * JanusGraph properties on your Schemas.
     *
     * @param property
     * @throws GraphIntegrityException
     * @throws Exception
     */
    public void createProperty(PrimitivePropertyKey property) throws GraphIntegrityException, Exception {
        if (!config.isAllowInternalFieldActions()) {
            verifyNotReservedField(property.getName());
        }
        try ( CloseableResourceLock lock = acquireLock(property)) {
            // rectify if needed
            rectifyPropertyIfNeeded(property.getName());
            // create record node of property (used to ensure a unique field for both primitive and ref props)
            try ( GraphTraversalSource g = getTraversalSource()) {
                try ( Transaction tx = g.tx()) {
                    String propNodeId = (String) addVertex(g, property.getId()).next().id();
                    g.V(propNodeId).property(PROPERTY_NAME, property.getName()).next();
                    String allowableValFieldKey = property.getAllowableValues().isEmpty()
                            ? null : calcAllowableValFieldNameFromType(property.getAllowableValues().iterator().next());
                    for (Object allowableVal : property.getAllowableValues()) {
                        g.V(propNodeId).property(allowableValFieldKey, allowableVal).next();
                    }
                    tx.commit();
                }
            }
            // if the above transaction worked, then create an JanusGraph actual property
            try ( JGMgntProvider mgnt = useManagement()) {
                JanusGraphManagement management = mgnt.getMgnt();
                PropertyKey key = management.makePropertyKey(property.getName())
                        .dataType(property.getDataType()).cardinality(property.getCardinality().getCardinality()).make();
                management.setConsistency(key, ConsistencyModifier.LOCK);
            }
        }
    }

    /**
     * Retrieve a list of all primitive properties in the system
     *
     * @return list of all primitive properties
     * @throws Exception generic JanusGraph exception
     */
    public List<PrimitivePropertyKey> listPrimitiveProps() throws Exception {
        List<PrimitivePropertyKey> outProps = new ArrayList<>();
        try ( JGMgntProvider mgnt = useManagement()) {
            try ( GraphTraversalSource g = getTraversalSource()) {
                getVertices(g).forEachRemaining(v -> {
                    String propName = (String) v.property(PropertyRepository.PROPERTY_NAME).value();
                    Optional<PrimitivePropertyKey> readProp = readPrimitiveProperty(propName, mgnt.getMgnt(), v);
                    if (readProp.isPresent()) {
                        outProps.add(readProp.get());
                    }
                });
            }
        }
        return outProps;
    }

    /**
     * Reads a primitive property & returns a populated optional if it exists.
     * An empty optional is returned if it does not.
     *
     * @param name property to lookup the object for
     * @return Optional of the looked up object
     * @throws java.lang.Exception generic JanusGraph exception
     */
    public Optional<PrimitivePropertyKey> readPrimitiveProperty(String name) throws Exception {
        try ( GraphTraversalSource g = getTraversalSource()) {
            GraphTraversal<Vertex, Vertex> search = getVertex(name, g);
            // ensure we're reading the right property type
            if (!search.hasNext()) {
                return Optional.empty();
            }
            Vertex vertex = search.next();
            verifyVertexIsType(vertex);
            try ( JGMgntProvider mgnt = useManagement()) {
                return readPrimitiveProperty(name, mgnt.getMgnt(), vertex);
            }
        }
    }

    private Optional<PrimitivePropertyKey> readPrimitiveProperty(String name, JanusGraphManagement management, Vertex v) {
        // ensure prop exists in the schema
        PropertyKey out = management.getPropertyKey(name);
        if (out == null) {
            return Optional.empty();
        }
        return Optional.of(new PrimitiveRepoProperty(out, v));
    }

    private GraphTraversal<Vertex, Vertex> getVertex(String name, GraphTraversalSource g) {
        return getVertices(g).has(PROPERTY_NAME, name);
    }

    private GraphTraversal<Vertex, Vertex> getVertices(GraphTraversalSource g) {
        return g.V().has(OFSRepository.NODE_TYPE_FIELD, VertexType.PRIM_PROPERTY.name());
    }

    // this could use the cool new java obj switch statements
    // but I don't want to couple this to a super high java version
    private String calcAllowableValFieldNameFromType(Object obj) throws GraphIntegrityException {
        if (obj instanceof String) {
            return ALLOWABLE_STRING_FIELD;
        }
        if (obj instanceof Character) {
            return ALLOWABLE_CHARACTER_FIELD;
        }
        if (obj instanceof Boolean) {
            return ALLOWABLE_BOOLEAN_FIELD;
        }
        if (obj instanceof Byte) {
            return ALLOWABLE_BYTE_FIELD;
        }
        if (obj instanceof Short) {
            return ALLOWABLE_SHORT_FIELD;
        }
        if (obj instanceof Integer) {
            return ALLOWABLE_INTEGER_FIELD;
        }
        if (obj instanceof Long) {
            return ALLOWABLE_LONG_FIELD;
        }
        if (obj instanceof Float) {
            return ALLOWABLE_FLOAT_FIELD;
        }
        if (obj instanceof Double) {
            return ALLOWABLE_DOUBLE_FIELD;
        }
        if (obj instanceof Date) {
            return ALLOWABLE_DATE_FIELD;
        }
        if (obj instanceof Geoshape) {
            return ALLOWABLE_GEOSHAPE_FIELD;
        }
        if (obj instanceof UUID) {
            return ALLOWABLE_UUID_FIELD;
        }
        throw new GraphIntegrityException("Attempted to use an invalid type for an allowable field");
    }

}
