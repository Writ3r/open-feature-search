/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api;

import java.util.Iterator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * Iterator over a GraphTraversal which transforms the Vertices into OFS objects
 * 
 * @author Lucas Wing
 * @param <E> type of vertex to iterate
 */
public abstract class VertexIterator<E extends OFSIdVertex> implements Iterator<E> {

    protected GraphTraversal<Vertex, Vertex> traversal;
    
    protected GraphTraversalSource g;

    protected VertexIterator(GraphTraversal<Vertex, Vertex> traversal, GraphTraversalSource g) {
        this.traversal = traversal;
        this.g = g;
    }

    @Override
    public boolean hasNext() {
        return traversal.hasNext();
    }

}

