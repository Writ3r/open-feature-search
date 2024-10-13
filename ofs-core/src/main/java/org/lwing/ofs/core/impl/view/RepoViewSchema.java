/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.view;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.lwing.ofs.core.api.schema.ViewSchema;
import org.lwing.ofs.core.impl.schema.RepoSchemaHelper;

/**
 *
 * @author Lucas Wing
 */
public class RepoViewSchema extends ViewSchema {

    public RepoViewSchema(Vertex v, GraphTraversalSource g) {
        super((String) v.id(), RepoSchemaHelper.getPropertyKeys(v), RepoSchemaHelper.getProperties(v, g));
    }
    
}
