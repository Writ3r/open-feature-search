/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.view;

import org.lwing.ofs.core.api.view.View;
import org.lwing.ofs.core.impl.PropertyUtil;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 *
 * @author Lucas Wing
 */
public class RepoView extends View {
    
    public RepoView(Vertex v, Optional<Set<String>> select, GraphTraversalSource g) {
        super((String) v.id(), getModels(v), getViews(v), PropertyUtil.getProperties(v, select, g), calcViewSchemaId(v));
    }
    
    private static Set<String> getViews(Vertex v) {
        Set<String> viewIds = new HashSet<>();
        v.edges(Direction.OUT, ViewRepository.VIEWS_VIEW_LABEL).forEachRemaining(e -> {
            Vertex out = e.inVertex();
            viewIds.add((String) out.id());
        });
        return viewIds;
    }
    
    private static Set<String> getModels(Vertex v) {
        Set<String> modelIds = new HashSet<>();
        v.edges(Direction.OUT, ViewRepository.VIEWS_MODEL_LABEL).forEachRemaining(e -> {
            Vertex out = e.inVertex();
            modelIds.add((String) out.id());
        });
        return modelIds;
    }
    
    private static String calcViewSchemaId(Vertex v) {
        return (String) v.edges(Direction.OUT, ViewRepository.VIEW_SCHEMA).next().inVertex().id();
    }
    
}
