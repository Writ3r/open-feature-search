/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.index;

import org.lwing.ofs.core.api.index.IndexType;
import org.lwing.ofs.core.api.index.Index;
import org.lwing.ofs.core.api.index.IndexElementType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.types.ParameterType;

/**
 *
 * @author Lucas Wing
 */
public class RepoIndex extends Index {

    public RepoIndex(JanusGraphIndex index) {
        super(index.name(), getIndexProps(index), IndexElementType.buildElementType(index.getIndexedElement()), index.isCompositeIndex()
                ? IndexType.COMPOSITE : IndexType.MIXED, index.isUnique(), getIndexStatusMap(index));
    }

    private static Map<String, Parameter[]> getIndexProps(JanusGraphIndex index) {
        Map<String, Parameter[]> properties = new HashMap<>();
        for (PropertyKey prop : Arrays.asList(index.getFieldKeys())) {
            Parameter[] parmas = index.getParametersFor(prop);
            Object mapping = ParameterType.MAPPING.findParameter(parmas, null);
            if (mapping != null) {
                properties.put(prop.name(), new Parameter[]{ParameterType.MAPPING.getParameter(mapping)});
            } else {
                properties.put(prop.name(), null);
            }
        }
        return properties;
    }

    private static Map<String, SchemaStatus> getIndexStatusMap(JanusGraphIndex index) {
        Map<String, SchemaStatus> statusMap = new HashMap<>();
        for (PropertyKey prop : index.getFieldKeys()) {
            statusMap.put(prop.name(), index.getIndexStatus(prop));
        }
        return statusMap;
    }

}
