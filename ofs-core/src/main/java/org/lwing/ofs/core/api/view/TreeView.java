/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.view;

import org.lwing.ofs.core.api.model.Model;
import org.lwing.ofs.core.impl.model.RepoModel;
import org.lwing.ofs.core.impl.view.RepoView;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * Contains all Views and Models under the source view. This is the entire Tree of data.
 * 
 * @author Lucas Wing
 */
public class TreeView extends RepoView {
    
    private final Set<TreeView> childViews;
    
    private final Set<Model> childModels;

    public TreeView(Vertex v, GraphTraversalSource g, Optional<Set<String>> modelSelect, Optional<Set<String>> viewSelect) {
        super(v, viewSelect, g);
        this.childViews = deriveViews(g, modelSelect, viewSelect);
        this.childModels = deriveModels(g, modelSelect);
    }
    
    private Set<TreeView> deriveViews(GraphTraversalSource g, Optional<Set<String>> modelSelect, Optional<Set<String>> viewSelect) {
        return getViewIds().stream().map(viewId -> {
            return new TreeView(g.V(viewId).next(), g, modelSelect, viewSelect);
        }).collect(Collectors.toSet());
    }
    
    private Set<Model> deriveModels(GraphTraversalSource g, Optional<Set<String>> modelSelect) {
        return getModelIds().stream().map(modelId -> {
            return new RepoModel(g.V(modelId).next(), modelSelect, g);
        }).collect(Collectors.toSet());
    }

    public Set<TreeView> getChildViews() {
        return childViews;
    }

    public Set<Model> getChildModels() {
        return childModels;
    }
    
}
