/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.feature;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.lwing.ofs.core.api.PropertiesVertex;
import org.lwing.ofs.core.api.property.Property;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.lwing.ofs.core.api.state.DependencyResource;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;

/**
 * Object which represents something intended to store & later search for.
 * Features must implement all fields through the inheritance of their Models.
 *
 * @author Lucas Wing
 */
public class Feature extends PropertiesVertex {

    // what model the feature inherits fields from
    private final String modelId;

    // calculated field after initial addition used to speed up searches
    private final Set<String> inheritsFrom;
    
    public Feature(String modelId, List<Property> properties) {
        super(null, properties);
        this.modelId = modelId;
        this.inheritsFrom = new HashSet<>();
    }
    
    public Feature(
            String featureId, 
            String modelId, 
            List<Property> properties
    ) {
        super(featureId, properties);
        this.modelId = modelId;
        this.inheritsFrom = new HashSet<>();
    }
    
    public Feature(
            @JsonProperty("featureId") String featureId, 
            @JsonProperty("modelId") String modelId, 
            @JsonProperty("properties") List<Property> properties,
            @JsonProperty("inheritsFrom") Set<String> inheritsFrom
    ) {
        super(featureId, properties);
        this.modelId = modelId;
        this.inheritsFrom = inheritsFrom;
    }
    
    public String getModelId() {
        return modelId;
    }
    
    public Set<String> getInheritsFrom() {
        return inheritsFrom;
    }
    
    @Override
    public Set<DependencyResource> calcDependencies() {
        Set<DependencyResource> resourcesToLock = new HashSet();
        resourcesToLock.add(DependencyResource.fromNodeId(OFSType.MODEL, modelId));
        if (getId() != null) {
            resourcesToLock.add(calcResource());
        }
        return resourcesToLock;
    }
    
    @Override
    public DependencyResource calcResource() {
        return DependencyResource.fromNodeId(OFSType.FEATURE, getId());
    }
    
}
