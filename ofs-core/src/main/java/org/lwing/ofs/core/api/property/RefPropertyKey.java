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
 * Property used for referencing models
 *
 * @author Lucas Wing
 */
public class RefPropertyKey extends JProperty<String> {

    private final String modelId;

    public RefPropertyKey(String name, String modelId, Cardinality cardinality) {
        super(name, PropType.REF, cardinality);
        this.modelId = modelId;
    }

    public RefPropertyKey(String name, String modelId, Cardinality cardinality, Set<String> allowableValues) {
        super(name, PropType.REF, cardinality, allowableValues);
        this.modelId = modelId;
    }

    public RefPropertyKey(
            @JsonProperty("nodeId") String nodeId, 
            @JsonProperty("name") String name, 
            @JsonProperty("modelId") String modelId, 
            @JsonProperty("cardinality") Cardinality cardinality, 
            @JsonProperty("allowableValues") Set<String> allowableValues
    ) {
        super(nodeId, name, PropType.REF, cardinality, allowableValues);
        this.modelId = modelId;
    }

    public String getModelId() {
        return modelId;
    }

    @Override
    public DependencyResource calcResource() {
        return DependencyResource.fromProperty(OFSType.REF_PROPERTY, getName());
    }

    @Override
    public Set<DependencyResource> calcDependencies() {
        return new HashSet(Arrays.asList(DependencyResource.fromNodeId(OFSType.MODEL, modelId), 
                DependencyResource.fromProperty(OFSType.JPROPERTY, getName())));
    }

}
