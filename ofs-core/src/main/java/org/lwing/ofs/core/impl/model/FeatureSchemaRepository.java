/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.model;

import java.util.Optional;
import java.util.Set;
import org.lwing.ofs.core.api.config.OFSConfiguration;
import org.lwing.ofs.core.api.VertexType;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.exception.InternalKeywordException;
import org.lwing.ofs.core.impl.property.PropertyRepository;
import org.lwing.ofs.core.impl.schema.SchemaRepository;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.lwing.ofs.core.api.schema.FeatureSchema;

/**
 *
 * @author Lucas Wing
 */
public class FeatureSchemaRepository extends SchemaRepository<FeatureSchema> {

    public FeatureSchemaRepository(JanusGraph graph, PropertyRepository popertyRepository, OFSConfiguration config) {
        super(VertexType.FEATURE_SCHEMA, graph, popertyRepository, config);
    }

    @Override
    protected String createSchema(FeatureSchema schema, GraphTraversalSource g) throws InternalKeywordException, Exception {
        return super.createSchema(schema, g);
    }

    /**
     * Deletes the schema with the input id from the configured
     * GraphTraversalSource
     *
     * @param schemaId identifier of the Schema to delete
     * @throws GraphIntegrityException
     * @throws Exception
     */
    @Override
    protected void deleteSchema(String schemaId, GraphTraversalSource g) throws GraphIntegrityException, Exception {
        super.deleteSchema(schemaId, g);
    }

    @Override
    protected FeatureSchema buildType(Vertex v, GraphTraversalSource g, Optional<Set<String>> select) {
        return new RepoFeatureSchema(v, g);
    }

}
