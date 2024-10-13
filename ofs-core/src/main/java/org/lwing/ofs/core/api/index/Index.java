/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.index;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.SchemaStatus;
import org.lwing.ofs.core.api.state.DependencyResource;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;
import org.lwing.ofs.core.api.state.StatefulResource;

/**
 * Object used to create Indices within JanusGraph
 * 
 * @author Lucas Wing
 */
public class Index implements StatefulResource {
    
    private final String name;
    
    private final Map<String, Parameter[]> properties;
    
    private final IndexElementType fieldType;
    
    private final IndexType indexType;
    
    private final Map<String, SchemaStatus> propertyIndexStatus;
    
    private final boolean unique;

    public Index(
            @JsonProperty("name") String name, 
            @JsonProperty("properties") Map<String, Parameter[]> properties, 
            @JsonProperty("fieldType") IndexElementType fieldType, 
            @JsonProperty("indexType") IndexType indexType, 
            @JsonProperty("unique") boolean unique, 
            @JsonProperty("propertyIndexStatus") Map<String, SchemaStatus> propertyIndexStatus
    ) {
        this.name = name;
        this.properties = properties;
        this.fieldType = fieldType;
        this.indexType = indexType;
        this.unique = unique;
        this.propertyIndexStatus = propertyIndexStatus;
    }
    
    public Index(String name, Map<String, Parameter[]> properties, IndexElementType fieldType, IndexType indexType, boolean unique) {
        this.name = name;
        this.properties = properties;
        this.fieldType = fieldType;
        this.indexType = indexType;
        this.unique = unique;
        this.propertyIndexStatus = null;
    }

    public Map<String, Parameter[]> getProperties() {
        return properties;
    }

    public String getName() {
        return name;
    }

    public IndexElementType getFieldType() {
        return fieldType;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public boolean isUnique() {
        return unique;
    }

    public Map<String, SchemaStatus> getPropertyIndexStatus() {
        return propertyIndexStatus;
    }

    @Override
    public int hashCode() {
        return this.name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Index)) {
            return false;
        }
        final Index other = (Index) obj;
        return Objects.equals(this.name, other.name);
    }

    @Override
    public Set<DependencyResource> calcDependencies() {
        return properties.keySet().stream().map(p -> DependencyResource.fromProperty(OFSType.JPROPERTY, p))
                .collect(Collectors.toSet());
    }
    
    @Override
    public DependencyResource calcResource() {
        return DependencyResource.fromIndex(getName());
    }
    
}
