/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.property;

import java.util.HashSet;
import java.util.Set;
import org.lwing.ofs.core.api.OFSIdVertex;
import org.lwing.ofs.core.api.state.StatefulResource;

/**
 * Superclass which custom property types need to extend in order to
 * allow for cohesive operations
 * 
 * @author Lucas Wing
 * @param <E>
 */
public abstract class JProperty<E> extends PropertyKey implements OFSIdVertex, StatefulResource {

    private final PropType propType;

    private final Cardinality cardinality;
    
    private final Set<E> allowableValues;
    
    private final String id;

    public enum PropType {
        PRIMITIVE,
        REF,
        NOT_FOUND
    }

    protected JProperty(String name, PropType propType, Cardinality cardinality) {
        super(name);
        this.propType = propType;
        this.cardinality = cardinality;
        this.allowableValues = new HashSet<>();
        this.id = null;
    }
    
    protected JProperty(String name, PropType propType, Cardinality cardinality, Set<E> allowableValues) {
        super(name);
        this.propType = propType;
        this.cardinality = cardinality;
        this.allowableValues = allowableValues;
        this.id = null;
    }
    
    protected JProperty(String nodeId, String name, PropType propType, Cardinality cardinality, Set<E> allowableValues) {
        super(name);
        this.propType = propType;
        this.cardinality = cardinality;
        this.allowableValues = allowableValues;
        this.id = nodeId;
    }

    public PropType getPropType() {
        return propType;
    }

    public Cardinality getCardinality() {
        return cardinality;
    }

    public Set<E> getAllowableValues() {
        return allowableValues;
    }
    
    @Override
    public String getId() {
        return id;
    }

}
