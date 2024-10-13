/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.view;

import org.lwing.ofs.core.api.model.Model;
import org.lwing.ofs.core.api.view.View;
import org.lwing.ofs.core.api.view.ViewLevel;
import org.lwing.ofs.core.impl.model.RepoModel;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

/**
 *
 * @author Lucas Wing
 */
public class RepoViewLevel extends ViewLevel {

    public RepoViewLevel(View view, GraphTraversalSource g, Optional<Set<String>> viewSelect, Optional<Set<String>> modelSelect) {
        super(view.getId(), deriveModels(view, g, modelSelect), deriveViews(view, g, viewSelect));
    }
    
    private static Set<View> deriveViews(View view, GraphTraversalSource g, Optional<Set<String>> select) {
        return view.getViewIds().stream().map(viewId -> {
            return new RepoView(g.V(viewId).next(), select, g);
        }).collect(Collectors.toSet());
    }
    
    private static Set<Model> deriveModels(View view, GraphTraversalSource g, Optional<Set<String>> select) {
        return view.getModelIds().stream().map(modelId -> {
            return new RepoModel(g.V(modelId).next(), select, g);
        }).collect(Collectors.toSet());
    }
    
}
