/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.model;

import org.lwing.ofs.core.api.model.Model;
import org.lwing.ofs.core.impl.PropertyUtil;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.lwing.ofs.core.api.schema.FeatureSchema;

/**
 *
 * @author Lucas Wing
 */
public class RepoModel extends Model {

    public RepoModel(Vertex v, Optional<Set<String>> select, GraphTraversalSource g) {
        super((String) v.id(), getInheritsFrom(v), PropertyUtil.getProperties(v, select, g), calcModelSchemaId(v), calcFeatureSchema(v, g));
    }

    private static String calcModelSchemaId(Vertex v) {
        return (String) v.edges(Direction.OUT, ModelRepository.MODEL_SCHEMA).next().inVertex().id();
    }

    private static FeatureSchema calcFeatureSchema(Vertex v, GraphTraversalSource g) {
        Vertex featureSchema = v.edges(Direction.OUT, ModelRepository.FEATURE_SCHEMA).next().inVertex();
        return new RepoFeatureSchema(featureSchema, g);
    }

    private static Set<String> getInheritsFrom(Vertex v) {
        Set<String> edgeInherits = new HashSet<>();
        v.edges(Direction.OUT, ModelRepository.EXTENDS_EDGE_ID).forEachRemaining(e -> {
            edgeInherits.add((String) e.inVertex().id());
        });
        return edgeInherits;
    }

}
