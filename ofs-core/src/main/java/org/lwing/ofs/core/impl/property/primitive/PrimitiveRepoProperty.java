/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.property.primitive;

import org.lwing.ofs.core.api.property.Cardinality;
import org.lwing.ofs.core.api.property.PrimitivePropertyKey;
import static org.lwing.ofs.core.impl.property.primitive.PrimitivePropertyRepository.ALLOWABLE_BOOLEAN_FIELD;
import static org.lwing.ofs.core.impl.property.primitive.PrimitivePropertyRepository.ALLOWABLE_BYTE_FIELD;
import static org.lwing.ofs.core.impl.property.primitive.PrimitivePropertyRepository.ALLOWABLE_CHARACTER_FIELD;
import static org.lwing.ofs.core.impl.property.primitive.PrimitivePropertyRepository.ALLOWABLE_DATE_FIELD;
import static org.lwing.ofs.core.impl.property.primitive.PrimitivePropertyRepository.ALLOWABLE_DOUBLE_FIELD;
import static org.lwing.ofs.core.impl.property.primitive.PrimitivePropertyRepository.ALLOWABLE_FLOAT_FIELD;
import static org.lwing.ofs.core.impl.property.primitive.PrimitivePropertyRepository.ALLOWABLE_GEOSHAPE_FIELD;
import static org.lwing.ofs.core.impl.property.primitive.PrimitivePropertyRepository.ALLOWABLE_INTEGER_FIELD;
import static org.lwing.ofs.core.impl.property.primitive.PrimitivePropertyRepository.ALLOWABLE_LONG_FIELD;
import static org.lwing.ofs.core.impl.property.primitive.PrimitivePropertyRepository.ALLOWABLE_SHORT_FIELD;
import static org.lwing.ofs.core.impl.property.primitive.PrimitivePropertyRepository.ALLOWABLE_STRING_FIELD;
import static org.lwing.ofs.core.impl.property.primitive.PrimitivePropertyRepository.ALLOWABLE_UUID_FIELD;
import java.util.HashSet;
import java.util.Set;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.PropertyKey;

/**
 *
 * @author Lucas Wing
 */
public class PrimitiveRepoProperty extends PrimitivePropertyKey {

    public PrimitiveRepoProperty(PropertyKey prop, Vertex v) {
        super(calcVertexId(v), prop.name(), prop.dataType(), Cardinality.valueFrom(prop.cardinality()), calcAllowableValues(v));
    }

    private static Set<Object> calcAllowableValues(Vertex v) {
        Set<Object> allowableValues = new HashSet<>();
        if (v != null) {
            v.properties(ALLOWABLE_STRING_FIELD, ALLOWABLE_CHARACTER_FIELD,
                    ALLOWABLE_BOOLEAN_FIELD, ALLOWABLE_BYTE_FIELD, ALLOWABLE_SHORT_FIELD,
                    ALLOWABLE_INTEGER_FIELD, ALLOWABLE_LONG_FIELD, ALLOWABLE_FLOAT_FIELD,
                    ALLOWABLE_DOUBLE_FIELD, ALLOWABLE_DATE_FIELD, ALLOWABLE_GEOSHAPE_FIELD,
                    ALLOWABLE_UUID_FIELD
            ).forEachRemaining(p -> {
                allowableValues.add(p.value());
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
