/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.model;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.lwing.ofs.core.api.schema.ModelSchema;
import org.lwing.ofs.core.impl.schema.RepoSchemaHelper;

/**
 *
 * @author Lucas Wing
 */
public class RepoModelSchema extends ModelSchema {

    public RepoModelSchema(Vertex v, GraphTraversalSource g) {
        super((String) v.id(), RepoSchemaHelper.getPropertyKeys(v), RepoSchemaHelper.getProperties(v, g));
    }
    
}
