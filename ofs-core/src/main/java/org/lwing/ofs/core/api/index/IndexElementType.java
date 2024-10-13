/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.index;

import java.util.Arrays;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * Wrapper around the JanusGraph index element types.
 * This is here in case there's different element types JanusGraph has/adds which OFS does not support.
 * 
 * @author Lucas Wing
 */
public enum IndexElementType {
    
    VERTEX(Vertex.class),
    EDGE(Edge.class);
    
    Class<? extends Element> elementType;
    
    private IndexElementType(Class<? extends Element> elementType) {
        this.elementType = elementType;
    }

    public Class<? extends Element> getElementType() {
        return elementType;
    }
    
    public static IndexElementType buildElementType(Class<? extends Element> elementType) {
        for (IndexElementType type: Arrays.asList(IndexElementType.values())) {
            type.getElementType().isAssignableFrom(elementType);
            if (type.getElementType().isAssignableFrom(elementType)) {
                return type;
            }
        }
        return null;
    }
    
}
