/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.feature;

import org.lwing.ofs.core.api.feature.Feature;
import org.lwing.ofs.core.api.config.OFSConfiguration;
import org.lwing.ofs.core.impl.PropertyUtil;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 *
 * @author Lucas Wing
 */
public class RepoFeature extends Feature {

    public RepoFeature(Vertex v, Optional<Set<String>> select, GraphTraversalSource g) {
        super((String) v.id(), deriveModelId(v), PropertyUtil.getProperties(v, select, g), calcInheritsFrom(v));
    }
    
    private static String deriveModelId(Vertex v) {
        return (String) v.property(OFSConfiguration.MODEL_ID).value();
    }
    
     private static Set<String> calcInheritsFrom(Vertex v) {
        Set<String> propsOut = new HashSet<>();
        v.properties(FeatureRepository.INHERITS_FROM_PROP).forEachRemaining(p -> {
            propsOut.add((String) p.value());
        });
        return propsOut;
    }
    
}
