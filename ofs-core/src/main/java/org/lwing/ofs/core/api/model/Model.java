/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.lwing.ofs.core.api.PropertiesVertex;
import org.lwing.ofs.core.api.property.Property;
import java.util.List;
import java.util.Set;
import org.lwing.ofs.core.api.state.DependencyResource;
import java.util.HashSet;
import org.lwing.ofs.core.api.schema.FeatureSchema;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;

/**
 * Holds metadata & the schema object which Features must implement.
 * Models also can have properties and must implement the ones specified by the modelSchema.
 * 
 * @author Lucas Wing
 */
public class Model extends PropertiesVertex {

    private final Set<String> inheritsFromIds;
    private final FeatureSchema featureSchema;
    private final String modelSchemaId;

    public Model(Set<String> inheritsFromIds, List<Property> properties, String modelSchemaId, FeatureSchema featureSchema) {
        super(null, properties);
        this.inheritsFromIds = inheritsFromIds;
        this.modelSchemaId = modelSchemaId;
        this.featureSchema = featureSchema;
    }
    
    public Model(
            @JsonProperty("id") String id, 
            @JsonProperty("inheritsFromIds") Set<String> inheritsFromIds, 
            @JsonProperty("properties") List<Property> properties, 
            @JsonProperty("modelSchemaId") String modelSchemaId, 
            @JsonProperty("featureSchema") FeatureSchema featureSchema
    ) {
        super(id, properties);
        this.inheritsFromIds = inheritsFromIds;
        this.modelSchemaId = modelSchemaId;
        this.featureSchema = featureSchema;
    }

    public Set<String> getInheritsFromIds() {
        return inheritsFromIds;
    }

    public String getModelSchemaId() {
        return modelSchemaId;
    }

    public FeatureSchema getFeatureSchema() {
        return featureSchema;
    }
    
    @Override
    public Set<DependencyResource> calcDependencies() {
        Set<DependencyResource> resourcesToLock = new HashSet<>();
        resourcesToLock.addAll(DependencyResource.getNodeResources(OFSType.MODEL, inheritsFromIds));
        resourcesToLock.add(DependencyResource.fromNodeId(OFSType.MODEL_SCHEMA, modelSchemaId));
        if (getId() != null) {
            resourcesToLock.add(calcResource());
        }
        return resourcesToLock;
    }
    
    @Override
    public DependencyResource calcResource() {
        return DependencyResource.fromNodeId(OFSType.MODEL, getId());
    }

}
