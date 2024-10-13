/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Set;
import org.lwing.ofs.core.api.property.Property;
import org.lwing.ofs.core.api.state.DependencyResource;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;

/**
 *
 * @author Lucas Wing
 */
public class FeatureSchema extends Schema {

    public FeatureSchema(List<Property> defaultProperties) {
        super(defaultProperties);
    }

    public FeatureSchema(Set<String> propertyKeys) {
        super(propertyKeys);
    }

    public FeatureSchema(Set<String> propertyKeys, List<Property> defaultProperties) {
        super(propertyKeys, defaultProperties);
    }

    protected FeatureSchema(
            @JsonProperty("id") String id, 
            @JsonProperty("propertyKeys") Set<String> propertyKeys, 
            @JsonProperty("defaultProperties") List<Property> defaultProperties
    ) {
        super(id, propertyKeys, defaultProperties);
    }
    
    @Override
    public DependencyResource calcResource() {
        return DependencyResource.fromNodeId(OFSType.FEATURE_SCHEMA, getId());
    }
    
}
