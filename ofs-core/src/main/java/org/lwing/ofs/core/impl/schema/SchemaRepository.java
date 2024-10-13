/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.lwing.ofs.core.api.config.OFSConfiguration;
import static org.lwing.ofs.core.api.config.OFSConfiguration.INTERNAL_FIELD_PREFIX;
import org.lwing.ofs.core.api.VertexType;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.exception.InternalKeywordException;
import org.lwing.ofs.core.api.exception.VertexNotFoundException;
import org.lwing.ofs.core.api.property.JProperty;
import org.lwing.ofs.core.api.property.JProperty.PropType;
import org.lwing.ofs.core.api.property.Property;
import org.lwing.ofs.core.api.property.RefPropertyKey;
import org.lwing.ofs.core.api.schema.Schema;
import org.lwing.ofs.core.impl.PropertyUtil;
import static org.lwing.ofs.core.impl.PropertyUtil.verifyPropsAreAllowable;
import org.lwing.ofs.core.impl.SearchableVertexRepository;
import org.lwing.ofs.core.impl.TypeVerifier;
import org.lwing.ofs.core.impl.lock.CloseableResourceLock;
import org.lwing.ofs.core.impl.property.PropertyRepository;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.V;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraph;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;

/**
 *
 * @author Lucas Wing
 * @param <E> type of schema the repository is for
 */
public abstract class SchemaRepository<E extends Schema> extends SearchableVertexRepository<E> {

    // property on the ref edge which is the 'name' of the property
    public static final String REF_PROP_NAME_EDGE_PROP = OFSConfiguration.INTERNAL_FIELD_PREFIX + "schema_property_name";

    // edge which points at a default reference value for the schema
    public static final String REF_SCHEMA_DEFALT_EDGE = OFSConfiguration.INTERNAL_FIELD_PREFIX + "schema_property_default";

    // edge which points at each property used by a schema
    public static final String USED_SCHEMA_PROP_EDGE = OFSConfiguration.INTERNAL_FIELD_PREFIX + "used_schema_property";

    // field hich states which primitive properties are in use
    public static final String PRIMITIVE_PROPERTIES = INTERNAL_FIELD_PREFIX + "primitive_props";

    // field which states which reference properties are in use
    public static final String REFERENCE_PROPERTIES = INTERNAL_FIELD_PREFIX + "reference_props";

    private final PropertyRepository popertyRepository;

    protected SchemaRepository(VertexType schemaType, JanusGraph graph, PropertyRepository popertyRepository, OFSConfiguration config) {
        super(graph, config, schemaType);
        this.popertyRepository = popertyRepository;
    }

    /**
     * Stores a Schema object in the graph. Schemas are used to save what
     * properties and their defaults go on a certain node. This is similar to
     * how you'd represent in interface in Typescript (except with default props
     * as well).
     *
     * @param schema input Schema object
     * @return created Schema object's id
     * @throws InternalKeywordException if user attempts to create the schema
     * with an internal field
     * @throws Exception generic JanusGraph exception
     */
    protected String createSchema(E schema) throws InternalKeywordException, Exception {
        try ( GraphTraversalSource g = getTraversalSource()) {
            try ( Transaction tx = g.tx()) {
                String schemaId;
                // create
                schemaId = createSchema(schema, g);
                // done
                tx.commit();
                return schemaId;
            }
        }
    }

    /**
     * Internal way to create Schemas using an existing GraphTraversalSource
     *
     * @param schema input Schema object
     * @param g GraphTraversalSource to use
     * @return id of created Schema
     * @throws InternalKeywordException if user attempts to create the schema
     * with an internal field
     * @throws Exception generic JanusGraph exception
     */
    protected String createSchema(E schema, GraphTraversalSource g) throws InternalKeywordException, Exception {
        // pre-verifications on reserved props
        for (String prop : schema.getPropertyKeys()) {
            verifyNotReservedField(prop);
        }
        // start add vertex 
        GraphTraversal<Vertex, Vertex> schemaV = addVertex(g, schema.getId());
        Vertex schemaVOut = schemaV.next();
        // add attr properties
        try ( CloseableResourceLock lock = acquireLock(schema)) {
            addPropsToSchemaV(schema, (String) schemaVOut.id(), g);
        }
        // done
        return (String) schemaVOut.id();
    }

    /**
     * Reads a schema object from the GraphTraversalSource
     *
     * @param id unique identifier of the schema
     * @return object representation of the Schema
     * @throws VertexNotFoundException if vertex does not exist
     * @throws Exception generic JanusGraph exception
     */
    public E readSchema(String id) throws VertexNotFoundException, Exception {
        try ( GraphTraversalSource g = getTraversalSource()) {
            return readSchema(id, g);
        }
    }

    protected E readSchema(String id, GraphTraversalSource g) throws VertexNotFoundException, Exception {
        return buildType(readVertexFromGraph(id, g), g, PropertyUtil.ALL_SELECT);
    }

    /**
     * Deletes the schema with the input id from the configured
     * GraphTraversalSource
     *
     * @param schemaId identifier of the Schema to delete
     * @param type type of schema to delete
     * @throws GraphIntegrityException
     * @throws Exception
     */
    protected void deleteSchema(String schemaId, OFSType type) throws GraphIntegrityException, Exception {
        deleteVertex(schemaId, type);
    }
    
    /**
     * Deletes the schema with the input id from the configured
     * GraphTraversalSource
     *
     * @param schemaId identifier of the Schema to delete
     * @param g traversal source to use
     * @throws GraphIntegrityException
     * @throws Exception
     */
    protected void deleteSchema(String schemaId, GraphTraversalSource g) throws GraphIntegrityException, Exception {
        deleteVertex(schemaId, g);
    }

    private String addPropsToSchemaV(E schema, String schemaId, GraphTraversalSource g) throws JsonProcessingException, GraphIntegrityException, Exception {
        // build prop info map
        Map<String, JProperty> propInfo = buildPropInfoMap(schema.getPropertyKeys(), popertyRepository);
        // verify properties are allowed
        verifyPropsAreAllowable(schema.getDefaultProperties(), propInfo, true);
        // add links to all used properties
        for (JProperty prop : propInfo.values()) {
            g.addE(USED_SCHEMA_PROP_EDGE).from(V(schemaId)).to(V(prop.getId())).next();
        }
        // get vertex
        GraphTraversal<Vertex, Vertex> ver = g.V(schemaId);
        // add supported properties
        for (Entry<String, JProperty> entry : propInfo.entrySet()) {
            if (entry.getValue().getPropType() == PropType.PRIMITIVE) {
                ver.property(VertexProperty.Cardinality.set, PRIMITIVE_PROPERTIES, entry.getKey());
            } else if (entry.getValue().getPropType() == PropType.REF) {
                ver.property(VertexProperty.Cardinality.set, REFERENCE_PROPERTIES, entry.getKey());
            }
        }
        // add primitive props
        for (Property prop : getPropsOfType(schema.getDefaultProperties(), PropType.PRIMITIVE, propInfo)) {
            if (schema.getPropertyKeys().contains(prop.getName())) {
                ver.property(prop.getName(), prop.getValue());
            }
        }
        ver.next();
        // add reference props
        List<Property> refPropList = getPropsOfType(schema.getDefaultProperties(), PropType.REF, propInfo);
        Map<String, Set<String>> usedRefPropsMap = new HashMap<>();
        for (Property prop : refPropList) {
            // if prop is not in list of supported, move on.
            if (!schema.getPropertyKeys().contains(prop.getName())) {
                continue;
            }
            RefPropertyKey propKey = (RefPropertyKey) propInfo.get(prop.getName());
            // add ref default val if it exists
            Set<String> usedPropSet = usedRefPropsMap.getOrDefault(prop.getName(), new HashSet<>());
            if (prop.getValue() != null) {
                // verify schema
                TypeVerifier.verifyFeatureFitsProp((String) prop.getValue(), g, propKey.getModelId());
                // store value refed on vertex
                PropertyUtil.handleDefaultRefAddToFeature(propKey, g, schemaId, prop,
                        usedPropSet.contains((String) prop.getValue()), usedRefPropsMap.containsKey(prop.getName()), REF_SCHEMA_DEFALT_EDGE);
            }
            // store off used props (so we can tell if cardinality is being violated or not)
            // POSSIBLE TODO: use a schema label with a certain cardinality to tell that instead?
            usedPropSet.add((String) prop.getValue());
            usedRefPropsMap.put(prop.getName(), usedPropSet);
        }
        return schemaId;
    }

    private List<Property> getPropsOfType(List<Property> inList, PropType type, Map<String, JProperty> propInfo) throws Exception {
        List<Property> outList = new ArrayList<>();
        for (Property prop : inList) {
            PropType propType = propInfo.get(prop.getName()).getPropType();
            if (propType == PropType.NOT_FOUND) {
                throw new GraphIntegrityException("Property type of property: [%s] does not exist", prop.getName());
            }
            if (propInfo.get(prop.getName()).getPropType() == type) {
                outList.add(prop);
            }
        }
        return outList;
    }

    /**
     * Builds a map of a property string to the associated Property object. Used
     * mostly internally.
     *
     * @param propertyKeys
     * @param popertyRepository
     * @return
     * @throws Exception
     */
    public static Map<String, JProperty> buildPropInfoMap(Set<String> propertyKeys, PropertyRepository popertyRepository) throws Exception {
        Map<String, JProperty> propInfoMap = new HashMap<>();
        for (String propKey : propertyKeys) {
            propInfoMap.put(propKey, popertyRepository.getProperty(propKey));
        }
        return propInfoMap;
    }

}
