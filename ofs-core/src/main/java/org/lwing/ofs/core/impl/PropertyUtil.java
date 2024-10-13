/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl;

import org.lwing.ofs.core.api.config.OFSConfiguration;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.property.JProperty;
import org.lwing.ofs.core.api.property.Property;
import org.lwing.ofs.core.api.property.RefPropertyKey;
import static org.lwing.ofs.core.impl.SchemaVertexRepository.REF_PROP_EDGE;
import static org.lwing.ofs.core.impl.schema.SchemaRepository.REF_PROP_NAME_EDGE_PROP;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import static java.util.Objects.isNull;
import java.util.Optional;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.V;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 *
 * @author Lucas Wing
 */
public class PropertyUtil {

    public static final Optional<Set<String>> EMPTY_SELECT = Optional.of(new HashSet<>(Arrays.asList()));

    public static final Optional<Set<String>> ALL_SELECT = Optional.empty();

    private PropertyUtil() {
    }

    /**
     * Gets both primitive and reference properties off of a node and returns
     * them in a property list
     *
     * @param e element to pull properties off of
     * @param select fields to select off of the vertex
     * @param g traversal sources used to pull ref properties off of
     * @return list of primitive and reference properties
     */
    public static List<Property> getProperties(Vertex e, Optional<Set<String>> select, GraphTraversalSource g) {
        List<Property> out = new ArrayList<>();
        addPrimitiveProperties(e, out, select);
        addRefProperties(e, out, select, g);
        return out;
    }

    /**
     * Adds primitive properties from the input primitive property into the out list
     *
     * @param e input element to read properties from
     * @param out list to add properties to
     * @param select
     */
    public static void addPrimitiveProperties(Element e, List<Property> out, Optional<Set<String>> select) {
        String[] inputProps = select.isPresent() ? select.get().toArray(String[]::new) : null;
        e.properties(inputProps).forEachRemaining(p -> {
            if (!p.key().startsWith(OFSConfiguration.INTERNAL_FIELD_PREFIX)) {
                out.add(new Property(p.key(), p.value()));
            }
        });
    }

    private static void addRefProperties(Vertex e, List<Property> out, Optional<Set<String>> select, GraphTraversalSource g) {
        addRefProperties(e, out, select, g, REF_PROP_EDGE);
    }

    /**
     * Adds ref properties from the input ref property into the out list
     *
     * @param e
     * @param out
     * @param select
     * @param g
     * @param edgeName
     */
    public static void addRefProperties(Vertex e, List<Property> out, Optional<Set<String>> select, GraphTraversalSource g, String edgeName) {
        GraphTraversal<Vertex, Edge> outEdges = g.V(e.id()).outE(edgeName);
        if (!select.isEmpty()) {
            outEdges.has(REF_PROP_NAME_EDGE_PROP, P.within(select.get()));
        }
        outEdges.forEachRemaining(edge -> {
            String name = (String) edge.property(REF_PROP_NAME_EDGE_PROP).value();
            out.add(new Property(name, edge.inVertex().id()));
        });
    }

    public static void handleDefaultRefAddToFeature(RefPropertyKey propKey, GraphTraversalSource g, String vertexId, Property prop,
            boolean propValueUsedBefore, boolean propUsedBefore, String edgeName) throws GraphIntegrityException {
        switch (propKey.getCardinality()) {
            // if it's a list value, always add the default edge
            case LIST ->
                addEdgeToFeature(g, vertexId, prop, edgeName);
            // if it's a set value, only add it if it hasn't been used yet
            case SET -> {
                if (!propValueUsedBefore) {
                    addEdgeToFeature(g, vertexId, prop, edgeName);
                }
            }
            // if it's a single value, only add it if it hasn't been used yet
            case SINGLE -> {
                if (!propUsedBefore) {
                    addEdgeToFeature(g, vertexId, prop, edgeName);
                } else {
                    throw new GraphIntegrityException("Cannot add multiple properties to a single cardinality reference schema property [%s].", prop.getName());
                }
            }

        }
    }

    private static void addEdgeToFeature(GraphTraversalSource g, String schemaId, Property prop, String edgeName) {
        GraphTraversal<Edge, Edge> edge = g.addE(edgeName).from(V(schemaId)).to(V(prop.getValue()));
        edge = g.E(edge.next().id()).property(REF_PROP_NAME_EDGE_PROP, prop.getName());
        edge.next();
    }

    /**
     * Used to check if a input properties are allowed in the set of properties
     * as defined by the graph's schema
     *
     * @param inputProps user input props
     * @param propInfo map of prop name -> JanusGraph property obj
     * @param ignoreNull if null values should be ignored during the allowable
     * check (Schemas need to set null despite allowable values)
     * @throws GraphIntegrityException if the property is not allowed
     */
    public static void verifyPropsAreAllowable(
            List<? extends Property> inputProps,
            Map<String, JProperty> propInfo,
            boolean ignoreNull
    ) throws GraphIntegrityException {
        for (Property prop : inputProps) {
            verifyAllowableProperty(propInfo.get(prop.getName()), prop, ignoreNull);
        }
    }

    private static void verifyAllowableProperty(JProperty propSchema, Property prop, boolean ignoreNull) throws GraphIntegrityException {
        if (propSchema.getAllowableValues().isEmpty()) {
            return;
        }
        if (!propSchema.getAllowableValues().contains(prop.getValue())) {
            if (isNull(prop.getValue()) && ignoreNull) {
                return;
            }
            throw new GraphIntegrityException("Attempted to use a value [%s] which is not in the allowed set of values [%s]",
                    prop.getValue(), propSchema.getAllowableValues());
        }
    }

}
