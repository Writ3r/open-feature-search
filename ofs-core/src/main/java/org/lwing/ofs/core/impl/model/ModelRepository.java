/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.model;

import org.lwing.ofs.core.api.config.OFSConfiguration;
import static org.lwing.ofs.core.api.config.OFSConfiguration.INTERNAL_FIELD_PREFIX;
import org.lwing.ofs.core.api.VertexType;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.exception.VertexNotFoundException;
import org.lwing.ofs.core.api.model.Model;
import org.lwing.ofs.core.api.property.Property;
import org.lwing.ofs.core.api.schema.Schema;
import org.lwing.ofs.core.impl.PropertyUtil;
import org.lwing.ofs.core.impl.SchemaVertexRepository;
import org.lwing.ofs.core.impl.lock.CloseableResourceLock;
import org.lwing.ofs.core.impl.property.PropertyRepository;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.V;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.lwing.ofs.core.api.schema.FeatureSchema;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;

/**
 *
 * @author Lucas Wing
 */
public class ModelRepository extends SchemaVertexRepository<Model> {

    // pointer at model schema on models
    public static final String MODEL_SCHEMA = INTERNAL_FIELD_PREFIX + "model_schema";

    // pointer at feature schema on models
    public static final String FEATURE_SCHEMA = INTERNAL_FIELD_PREFIX + "feature_schema";

    // used specifically for models, if they 'extend' another model
    public static final String EXTENDS_EDGE_ID = INTERNAL_FIELD_PREFIX + "extends";

    private final ModelSchemaRepository modelSchemaRepo;

    private final FeatureSchemaRepository featureSchemaRepo;

    public ModelRepository(JanusGraph graph, ModelSchemaRepository modelSchemaRepo, FeatureSchemaRepository featureSchemaRepo, OFSConfiguration config, PropertyRepository propertyRepository) {
        super(VertexType.MODEL, graph, propertyRepository, config);
        this.modelSchemaRepo = modelSchemaRepo;
        this.featureSchemaRepo = featureSchemaRepo;
    }

    /**
     * Creates a Model object in the configured Graph. Models hold metadata and
     * Schemas which features that inherit from them must adhere to for their
     * fields.
     *
     * @param model input object
     * @return created model object's id
     * @throws GraphIntegrityException if there's missing properties
     * @throws Exception generic JanusGraph exception
     */
    public String createModel(Model model) throws GraphIntegrityException, Exception {
        try ( GraphTraversalSource g = getTraversalSource()) {
            try ( Transaction tx = g.tx()) {
                String modelId;
                try ( CloseableResourceLock lock = acquireLock(model)) {
                    // deal with schemas
                    Schema modelSchema = modelSchemaRepo.readSchema(model.getModelSchemaId());
                    List<Property> inpProps = calcPropsToUse(modelSchema, model.getProperties());
                    // start add vertex 
                    modelId = (String) addVertex(g, model.getId()).next().id();
                    // add attr properties
                    String featureSchemaId = featureSchemaRepo.createSchema(model.getFeatureSchema(), g);
                    addPropsToModelV(modelSchema, modelId, inpProps, g, featureSchemaId);
                    // add extends
                    for (String parentModelId : model.getInheritsFromIds()) {
                        g.addE(EXTENDS_EDGE_ID).from(V(modelId)).to(V(parentModelId)).next();
                    }
                    // done & return
                    tx.commit();
                }
                return modelId;
            }
        }
    }

    /**
     * Read an existing model using the stated id
     *
     * @param id unique identifier of the model node
     * @param select set of fields to return on the model
     * @return Model object
     * @throws Exception generic JanusGraph exception
     */
    public Model readModel(String id, Set<String> select) throws Exception {
        return readModel(id, Optional.of(select));
    }

    /**
     * Read an existing model using the stated id
     *
     * @param id unique identifier of the model node
     * @return Model object
     * @throws Exception generic JanusGraph exception
     */
    public Model readModel(String id) throws Exception {
        return readModel(id, PropertyUtil.ALL_SELECT);
    }

    /**
     * Read an existing model using the stated id
     *
     * @param id unique identifier of the model node
     * @param select optional of a set of fields to return on the model. Empty
     * optional means select all fields.
     * @return Model object
     * @throws Exception generic JanusGraph exception
     */
    public Model readModel(String id, Optional<Set<String>> select) throws Exception {
        try ( GraphTraversalSource g = getTraversalSource()) {
            return readModel(id, g, select);
        }
    }

    protected Model readModel(String id, GraphTraversalSource g, Optional<Set<String>> select) throws VertexNotFoundException, Exception {
        return new RepoModel(readVertexFromGraph(id, g), select, g);
    }

    /**
     * Returns a dynamically generated schema of properties which consist of all
     * which the model's feature schema and its supertypes require
     *
     * @param modelId id od the model to use
     * @return list of properties a feature must include (and their defaults)
     * @throws Exception generic JanusGraph exception
     */
    public Schema getRequiredFeatureProperties(String modelId) throws Exception {
        try ( GraphTraversalSource g = getTraversalSource()) {
            return getRequiredFeatureProperties(new HashSet<>(Arrays.asList(modelId)), new HashSet<>(), new ArrayList<>(), g);
        }
    }

    /**
     * Deletes the model associated with the input id from the
     * GraphTraversalSource along with the associated feature schema
     *
     * @param modelId id of model to delete
     * @throws GraphIntegrityException if the model is being referenced
     * @throws Exception generic JanusGraph exception
     */
    public void deleteModel(String modelId) throws GraphIntegrityException, Exception {
        try ( GraphTraversalSource g = getTraversalSource()) {
            try ( Transaction tx = g.tx()) {
                try ( CloseableResourceLock lock = acquireLock(OFSType.MODEL, modelId)) {
                    String featureSchemaId = readModel(modelId).getFeatureSchema().getId();
                    try ( CloseableResourceLock lock2 = acquireLock(OFSType.FEATURE_SCHEMA, featureSchemaId)) {
                        deleteVertex(modelId, g);
                        featureSchemaRepo.deleteSchema(featureSchemaId, g);
                        tx.commit();
                    }
                }
            }
        }
    }

    private void addPropsToModelV(Schema modelSchema, String modelId, List<Property> props, GraphTraversalSource g, String featureSchemaId) throws Exception {
        // add props to vertex
        addPropsToVertex(props, buildPropInfoMap(modelSchema), modelId, g);
        // add model schema link
        g.V(modelId).addE(MODEL_SCHEMA).to(V(modelSchema.getId())).next();
        // add feature schema link
        g.V(modelId).addE(FEATURE_SCHEMA).to(V(featureSchemaId)).next();
    }

    /**
     * Traverses up models adding the associated properties at each 'level' of
     * inheritance. Properties on a lower inheritance level override those
     * higher up. If two models are at the same level and provide the same
     * property, it's random what the value will be that's chosen to use (don't
     * do this lol).
     *
     * @param modelId id of model to delete
     * @throws GraphIntegrityException if the model is being referenced
     * @throws Exception generic JanusGraph exception
     */
    private Schema getRequiredFeatureProperties(
            Set<String> modelIds,
            Set<String> featurePropertyKeys,
            List<Property> featureDefaultProperties,
            GraphTraversalSource g
    ) throws Exception {
        Set<String> nextModelsToGetProperties = new HashSet<>();
        for (String id : modelIds) {
            Model model = readModel(id, g, PropertyUtil.EMPTY_SELECT);
            for (Property prop : model.getFeatureSchema().getDefaultProperties()) {
                if (!featurePropertyKeys.contains(prop.getName())) {
                    featureDefaultProperties.add(prop);
                }
            }
            featurePropertyKeys.addAll(model.getFeatureSchema().getPropertyKeys());
            nextModelsToGetProperties.addAll(model.getInheritsFromIds());
        }
        if (!nextModelsToGetProperties.isEmpty()) {
            getRequiredFeatureProperties(nextModelsToGetProperties, featurePropertyKeys, featureDefaultProperties, g);
        }
        return new FeatureSchema(featurePropertyKeys, featureDefaultProperties);
    }

    @Override
    protected Model buildType(Vertex v, GraphTraversalSource g, Optional<Set<String>> select) {
        return new RepoModel(v, select, g);
    }

}
