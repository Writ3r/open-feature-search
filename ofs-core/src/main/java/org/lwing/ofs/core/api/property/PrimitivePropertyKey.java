/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.property;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.lwing.ofs.core.api.state.DependencyResource;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;

/**
 * OFS Property used for primitive JanusGraph properties
 * 
 * @author Lucas Wing
 */
public class PrimitivePropertyKey<E> extends JProperty<E> {

    private final Class<E> dataType;
    
    public PrimitivePropertyKey(String name, Class<E> dataType, Cardinality cardinality) {
        super(name, PropType.PRIMITIVE, cardinality);
        this.dataType = dataType;
    }

    public PrimitivePropertyKey(String name, Class<E> dataType, Cardinality cardinality, Set<E> allowableValues) {
        super(name, PropType.PRIMITIVE, cardinality, allowableValues);
        this.dataType = dataType;
    }
    
    public PrimitivePropertyKey(
            @JsonProperty("nodeId") String nodeId, 
            @JsonProperty("name") String name, 
            @JsonProperty("dataType") Class<E> dataType, 
            @JsonProperty("cardinality") Cardinality cardinality, 
            @JsonProperty("allowableValues") Set<E> allowableValues
    ) {
        super(nodeId, name, PropType.PRIMITIVE, cardinality, allowableValues);
        this.dataType = dataType;
    }

    public Class<E> getDataType() {
        return dataType;
    }
    
    @Override
    public DependencyResource calcResource() {
        return DependencyResource.fromProperty(OFSType.PRIM_PROPERTY, getName());
    }

    @Override
    public Set<DependencyResource> calcDependencies() {
        return new HashSet(Arrays.asList(DependencyResource.fromProperty(OFSType.JPROPERTY, getName())));
    }

}
