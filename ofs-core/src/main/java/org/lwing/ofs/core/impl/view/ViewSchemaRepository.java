/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.view;

import java.util.Optional;
import java.util.Set;
import org.lwing.ofs.core.api.config.OFSConfiguration;
import org.lwing.ofs.core.api.VertexType;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.exception.InternalKeywordException;
import org.lwing.ofs.core.api.exception.VertexNotFoundException;
import org.lwing.ofs.core.impl.property.PropertyRepository;
import org.lwing.ofs.core.impl.schema.SchemaRepository;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.lwing.ofs.core.api.schema.ViewSchema;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;

/**
 *
 * @author Lucas Wing
 */
public class ViewSchemaRepository extends SchemaRepository<ViewSchema> {

    public ViewSchemaRepository(JanusGraph graph, PropertyRepository popertyRepository, OFSConfiguration config) {
        super(VertexType.VIEW_SCHEMA, graph, popertyRepository, config);
    }
    
    /**
     * Stores a Schema object in the graph. Schemas are used to save what
     * properties and their defaults go on a certain node. This is similar to
     * how you'd represent in interface in Typescript (except with default props
     * as well).
     *
     * @param schema input Schema object
     * @return created Schema object's id
     * @throws InternalKeywordException if user attempts to create the schema
     * with an internal field
     * @throws Exception generic JanusGraph exception
     */
    @Override
    public String createSchema(ViewSchema schema) throws InternalKeywordException, Exception {
        return super.createSchema(schema);
    }
    
    /**
     * Reads a schema object from the GraphTraversalSource
     *
     * @param id unique identifier of the schema
     * @param g traversal source to use
     * @return object representation of the Schema
     * @throws VertexNotFoundException if vertex does not exist
     * @throws Exception generic JanusGraph exception
     */
    @Override
    protected ViewSchema readSchema(String id, GraphTraversalSource g) throws VertexNotFoundException, Exception {
        return super.readSchema(id, g);
    }
    
    /**
     * Deletes the schema with the input id from the configured
     * GraphTraversalSource
     *
     * @param schemaId identifier of the Schema to delete
     * @throws GraphIntegrityException
     * @throws Exception
     */
    public void deleteSchema(String schemaId) throws GraphIntegrityException, Exception {
        super.deleteSchema(schemaId, OFSType.VIEW_SCHEMA);
    }
    
    @Override
    protected ViewSchema buildType(Vertex v, GraphTraversalSource g, Optional<Set<String>> select) {
        return new RepoViewSchema(v, g);
    }
    
}
