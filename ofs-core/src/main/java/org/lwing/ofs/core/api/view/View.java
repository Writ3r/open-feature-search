/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.view;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.lwing.ofs.core.api.PropertiesVertex;
import org.lwing.ofs.core.api.property.Property;
import java.util.List;
import java.util.Set;
import org.lwing.ofs.core.api.state.DependencyResource;
import java.util.HashSet;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;

/**
 * Consolidates Models and other Views into a tree based structure
 * to allow for dynamic ways to observe/search data
 * 
 * @author Lucas Wing
 */
public class View extends PropertiesVertex {

    private final Set<String> modelIds;
    private final Set<String> viewIds;
    private final String viewSchemaId;
    
    public View(Set<String> modelIds, Set<String> viewIds, List<Property> properties, String viewSchemaId) {
        super(null, properties);
        this.modelIds = modelIds;
        this.viewIds = viewIds;
        this.viewSchemaId = viewSchemaId;
    }
    
    public View(
            @JsonProperty("id") String id, 
            @JsonProperty("modelIds") Set<String> modelIds, 
            @JsonProperty("viewIds") Set<String> viewIds, 
            @JsonProperty("properties") List<Property> properties, 
            @JsonProperty("viewSchemaId") String viewSchemaId
    ) {
        super(id, properties);
        this.modelIds = modelIds;
        this.viewIds = viewIds;
        this.viewSchemaId = viewSchemaId;
    }

    public Set<String> getModelIds() {
        return modelIds;
    }

    public Set<String> getViewIds() {
        return viewIds;
    }

    public String getViewSchemaId() {
        return viewSchemaId;
    }

    @Override
    public Set<DependencyResource> calcDependencies() {
        Set<DependencyResource> resourcesToLock = new HashSet<>();
        resourcesToLock.addAll(DependencyResource.getNodeResources(OFSType.MODEL, modelIds));
        resourcesToLock.addAll(DependencyResource.getNodeResources(OFSType.VIEW, viewIds));
        resourcesToLock.add(DependencyResource.fromNodeId(OFSType.VIEW_SCHEMA, viewSchemaId));
        if (getId() != null) {
            resourcesToLock.add(calcResource());
        }
        return resourcesToLock;
    }

    @Override
    public DependencyResource calcResource() {
        return DependencyResource.fromNodeId(OFSType.VIEW, getId());
    }

}
