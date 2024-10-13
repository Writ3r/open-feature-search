/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.view;

import java.util.HashSet;
import org.lwing.ofs.core.api.OFSVertex;
import org.lwing.ofs.core.api.model.Model;
import java.util.Set;
import org.lwing.ofs.core.api.state.DependencyResource;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;

/**
 * Shows all the Views and Models directly under a View
 * 
 * @author Lucas Wing
 */
public class ViewLevel extends OFSVertex {
    
    private final Set<Model> models;
    private final Set<View> views;
    
    protected ViewLevel(String id, Set<Model> models, Set<View> views) {
        super(id);
        this.models = models;
        this.views = views;
    }

    public Set<Model> getModels() {
        return models;
    }

    public Set<View> getViews() {
        return views;
    }

    @Override
    public Set<DependencyResource> calcDependencies() {
        return new HashSet<>();
    }

    @Override
    public DependencyResource calcResource() {
        return DependencyResource.fromNodeId(OFSType.VIEW, getId());
    }

}
