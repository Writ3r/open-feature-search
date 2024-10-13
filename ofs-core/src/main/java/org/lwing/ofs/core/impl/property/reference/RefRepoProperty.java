/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.property.reference;

import static org.lwing.ofs.core.api.config.OFSConfiguration.MODEL_ID;
import org.lwing.ofs.core.api.property.Cardinality;
import org.lwing.ofs.core.api.property.RefPropertyKey;
import org.lwing.ofs.core.impl.property.PropertyRepository;
import static org.lwing.ofs.core.impl.property.PropertyRepository.ALLOWABLE_REF_FIELD;
import java.util.HashSet;
import java.util.Set;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 *
 * @author Lucas Wing
 */
public class RefRepoProperty extends RefPropertyKey {

    public RefRepoProperty(Vertex v) {
        super(calcVertexId(v), calcPropName(v), calcModelId(v), calcPropCardinality(v), calcAllowableValues(v));
    }
    
    private static String calcPropName(Vertex v) {
        return (String) v.property(PropertyRepository.PROPERTY_NAME).value();
    }
    
    private static Cardinality calcPropCardinality(Vertex v) {
        return Cardinality.valueOf((String) v.property(PropertyRepository.REF_CARDINALITY).value());
    }
    
    private static String calcModelId(Vertex v) {
        return (String) v.property(MODEL_ID).value();
    }
    
    private static Set<String> calcAllowableValues(Vertex v) {
        Set<String> allowableValues = new HashSet<>();
        if (v != null) {
            v.edges(Direction.OUT, ALLOWABLE_REF_FIELD).forEachRemaining(e -> {
                allowableValues.add((String) e.inVertex().id());
            });
        }
        return allowableValues;
    }
    
    private static String calcVertexId(Vertex v) {
        if (v == null) {
            return null;
        }
        return (String) v.id();
    }
    
}
