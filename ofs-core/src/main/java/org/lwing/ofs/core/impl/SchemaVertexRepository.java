/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl;

import org.lwing.ofs.core.api.OFSVertex;
import org.lwing.ofs.core.api.config.OFSConfiguration;
import org.lwing.ofs.core.api.VertexType;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.property.JProperty;
import org.lwing.ofs.core.api.property.Property;
import org.lwing.ofs.core.api.property.RefPropertyKey;
import org.lwing.ofs.core.api.schema.Schema;
import static org.lwing.ofs.core.impl.PropertyUtil.verifyPropsAreAllowable;
import org.lwing.ofs.core.impl.property.PropertyRepository;
import org.lwing.ofs.core.impl.schema.SchemaRepository;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Lucas Wing
 * @param <E> vertex object the repository aims at encompassing
 */
public abstract class SchemaVertexRepository<E extends OFSVertex> extends SearchableVertexRepository<E> {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaVertexRepository.class);

    // edge pointing at the ref value of a vertex
    public static final String REF_PROP_EDGE = OFSConfiguration.INTERNAL_FIELD_PREFIX + "schema_property_ref";

    protected PropertyRepository popertyRepository;

    protected SchemaVertexRepository(VertexType schemaType, JanusGraph graph, PropertyRepository popertyRepository, OFSConfiguration config) {
        super(graph, config, schemaType);
        this.popertyRepository = popertyRepository;
    }

    /**
     * Multiple repositories (ex. FeatureRepository & ModelRepository) need the
     * ability to add properties onto a vertex based on their Schemas. This
     * method allows for that. It will set primitive props on the vertex & also
     * check + set reference properties on the vertex if they meet the Schema of
     * the reference property.
     *
     * Conditions for reference property Cardinality. If the property is a
     * cardinality of SINGLE, an exception will be thrown if multiple are passed
     * in. If the property is a cardinality of SET, duplicates of the values
     * will be ignored. If the property is a cardinality of LIST, all
     * properties will be set
     *
     * @see org.lwing.ofs.core.impl.feature.FeatureRepository
     * @see org.lwing.ofs.core.impl.model.ModelRepository
     * @param props props to be set on the vertex
     * @param propInfoMap map with associated information about the input props
     * @param vertexId vertex getting props set on it
     * @param g traversal source
     * @throws GraphIntegrityException thrown if reference cardinality is being
     * violated
     * @throws Exception generic JanusGraph search exception
     */
    protected void addPropsToVertex(List<Property> props, Map<String, JProperty> propInfoMap, String vertexId, GraphTraversalSource g) throws GraphIntegrityException, Exception {
        Map<String, Set<String>> usedRefPropsMap = new HashMap<>();
        // verify properties are allowed
        verifyPropsAreAllowable(props, propInfoMap, false);
        // iterate through all properties
        for (Property prop : props) {
            // get the type of a prop
            JProperty propSchema = propInfoMap.get(prop.getName());
            // add primitive property
            if (propSchema.getPropType() == JProperty.PropType.PRIMITIVE) {
                g.V(vertexId).property(prop.getName(), prop.getValue()).next();
            } // add reference properties
            else if (propSchema.getPropType() == JProperty.PropType.REF) {
                Set<String> usedRefPropSet = usedRefPropsMap.getOrDefault(prop.getName(), new HashSet<>());
                RefPropertyKey propKey = (RefPropertyKey) propSchema;
                // lookup field's model & check if feature is a subtype
                TypeVerifier.verifyFeatureFitsProp((String) prop.getValue(), g, propKey.getModelId());
                // store value refed on vertex
                PropertyUtil.handleDefaultRefAddToFeature(propKey, g, vertexId, prop, 
                        usedRefPropSet.contains((String) prop.getValue()), usedRefPropsMap.containsKey(prop.getName()), REF_PROP_EDGE);
                // store off used props (so we can tell if cardinality is being violated or not)
                usedRefPropSet.add((String) prop.getValue());
                usedRefPropsMap.put(prop.getName(), usedRefPropSet);
            }
        }
    }

    /**
     * Calculates which properties to use based on input SchemaProperty list and
     * user input Property list
     *
     * @param schema schema the props will be used against
     * @param userProps properties which will be decided for use against the
     * schema props
     * @return list of properties that should be used
     * @throws GraphIntegrityException if the schema requires certain
     * properties not provided by the user's input properties
     * @throws Exception generic JanusGraph exception
     */
    protected List<Property> calcPropsToUse(Schema schema, List<Property> userProps) throws GraphIntegrityException, Exception {
        Set<String> neededSchemaPropsSet = new HashSet<>(schema.getPropertyKeys());
        List<Property> schemaProps = new ArrayList<>(schema.getDefaultProperties());
        return calcPropsToUse(neededSchemaPropsSet, schemaProps, userProps);
    }
    
    protected List<Property> calcPropsToUse(Set<String> neededSchemaPropsSet, List<Property> schemaProps, List<Property> userProps) throws GraphIntegrityException, Exception {
        Set<String> userPropKeySet = userProps.stream().map(Property::getName).collect(Collectors.toSet());
        // only add prop if it shows up in the schema
        List<Property> outProps = userProps.stream().filter(p -> {
            return neededSchemaPropsSet.contains(p.getName());
        }).collect(Collectors.toList());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("User input properties to use: {}", outProps);
        }
        // add defaults from schema if prop does not exists in user props
        List<Property> neededDefaultProps = schemaProps.stream()
                .filter(p -> !userPropKeySet.contains(p.getName()))
                .filter(p -> p.getValue() != null)
                .collect(Collectors.toList());
        outProps.addAll(neededDefaultProps);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Schema default properties to use: {}", neededDefaultProps);
        }
        // verify schema is being applied correctly
        Set<String> usedProps = outProps.stream().map(Property::getName).collect(Collectors.toSet());
        neededSchemaPropsSet.removeAll(usedProps);
        if (!neededSchemaPropsSet.isEmpty()) {
            throw new GraphIntegrityException("Necessary schema properties are missing from input properties [%s]",
                    String.join(",", neededSchemaPropsSet));
        }
        return outProps;
    }
    
    public Map<String, JProperty> buildPropInfoMap(Schema schema) throws Exception {
        return buildPropInfoMap(schema.getPropertyKeys(), schema.getDefaultProperties());
    }

    public Map<String, JProperty> buildPropInfoMap(Set<String> neededSchemaPropsSet, List<Property> schemaPropList) throws Exception {
        return SchemaRepository.buildPropInfoMap(neededSchemaPropsSet, popertyRepository);
    }

}
