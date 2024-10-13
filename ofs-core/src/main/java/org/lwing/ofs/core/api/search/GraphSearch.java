/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.search;

import org.lwing.ofs.core.api.exception.GraphSearchException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * Search interface for OFS traversals
 * 
 * @author Lucas Wing
 */
@FunctionalInterface
public interface GraphSearch {
    public GraphTraversal<Vertex, Vertex> search(GraphTraversalSource g) throws GraphSearchException;
}
