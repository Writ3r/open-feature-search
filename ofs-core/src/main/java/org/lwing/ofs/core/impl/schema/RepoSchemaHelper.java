/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.lwing.ofs.core.api.property.Property;
import org.lwing.ofs.core.impl.PropertyUtil;
import static org.lwing.ofs.core.impl.schema.SchemaRepository.PRIMITIVE_PROPERTIES;
import static org.lwing.ofs.core.impl.schema.SchemaRepository.REFERENCE_PROPERTIES;
import static org.lwing.ofs.core.impl.schema.SchemaRepository.REF_SCHEMA_DEFALT_EDGE;

/**
 *
 * @author Lucas Wing
 */
public class RepoSchemaHelper {

    private RepoSchemaHelper() {
    }

    public static Set<String> getPropertyKeys(Vertex v) {
        Set<String> outSet = new HashSet<>();
        outSet.addAll(getSupportedProps(v, PRIMITIVE_PROPERTIES));
        outSet.addAll(getSupportedProps(v, REFERENCE_PROPERTIES));
        return outSet;
    }

    public static List<Property> getProperties(Vertex v, GraphTraversalSource g) {
        List<Property> outAttrs = new ArrayList<>();
        outAttrs.addAll(getPrimitiveProps(v));
        outAttrs.addAll(getRefProps(v, g));
        return outAttrs;
    }

    private static List<Property> getPrimitiveProps(Element e) {
        List<Property> outAttrs = new ArrayList<>();
        PropertyUtil.addPrimitiveProperties(e, outAttrs, Optional.empty());
        return outAttrs;
    }

    private static List<Property> getRefProps(Vertex v, GraphTraversalSource g) {
        List<Property> outAttrs = new ArrayList<>();
        PropertyUtil.addRefProperties(v, outAttrs, Optional.empty(), g, REF_SCHEMA_DEFALT_EDGE);
        return outAttrs;
    }

    private static Set<String> getSupportedProps(Element e, String propForSupported) {
        Set<String> supportedProps = new HashSet<>();
        e.properties(propForSupported).forEachRemaining(p -> {
            supportedProps.add((String) p.value());
        });
        return supportedProps;
    }
    
}
