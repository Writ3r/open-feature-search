/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.schema;

import org.lwing.ofs.core.api.OFSVertex;
import org.lwing.ofs.core.api.state.DependencyResource;
import org.lwing.ofs.core.api.property.Property;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;

/**
 * Equivalent to an 'interface' in object oriented programming, 
 * this specifies which properties nodes like Features are required to implement.
 * 
 * @author Lucas Wing
 */
public abstract class Schema extends OFSVertex {
    
    private final Set<String> propertyKeys;

    private final List<Property> defaultProperties;
    
    protected Schema(List<Property> defaultProperties) {
        super(null);
        this.propertyKeys = defaultProperties.stream()
                .map(Property::getName).collect(Collectors.toSet());
        this.defaultProperties = defaultProperties;
    }
    
    protected Schema(Set<String> propertyKeys) {
        super(null);
        this.propertyKeys = propertyKeys;
        this.defaultProperties = new ArrayList<>();
    }

    protected Schema(Set<String> propertyKeys, List<Property> defaultProperties) {
        super(null);
        this.propertyKeys = propertyKeys;
        this.defaultProperties = defaultProperties;
    }
    
    protected Schema(String id, Set<String> propertyKeys, List<Property> defaultProperties) {
        super(id);
        this.propertyKeys = propertyKeys;
        this.defaultProperties = defaultProperties;
    }

    public Set<String> getPropertyKeys() {
        return propertyKeys;
    }

    public List<Property> getDefaultProperties() {
        return defaultProperties;
    }

    @Override
    public Set<DependencyResource> calcDependencies() {
        return propertyKeys.stream().map(p -> DependencyResource.fromProperty(OFSType.JPROPERTY, p))
                .collect(Collectors.toSet());
    }

}
