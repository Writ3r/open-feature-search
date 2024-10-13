/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.feature;

import org.lwing.ofs.core.api.feature.Feature;
import org.lwing.ofs.core.api.config.OFSConfiguration;
import static org.lwing.ofs.core.api.config.OFSConfiguration.INTERNAL_FIELD_PREFIX;
import org.lwing.ofs.core.api.VertexType;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.exception.VertexNotFoundException;
import org.lwing.ofs.core.api.model.Model;
import org.lwing.ofs.core.api.property.Property;
import org.lwing.ofs.core.api.schema.Schema;
import org.lwing.ofs.core.impl.OFSRepository;
import org.lwing.ofs.core.impl.PropertyUtil;
import org.lwing.ofs.core.impl.SchemaVertexRepository;
import org.lwing.ofs.core.impl.lock.CloseableResourceLock;
import org.lwing.ofs.core.impl.model.ModelRepository;
import org.lwing.ofs.core.impl.model.RepoModel;
import org.lwing.ofs.core.impl.property.PropertyRepository;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.V;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;

/**
 *
 * @author Lucas Wing
 */
public class FeatureRepository extends SchemaVertexRepository<Feature> {

    private final ModelRepository modelRepo;

    public static final String INHERITS_FROM_PROP = INTERNAL_FIELD_PREFIX + "inheritsFrom";

    public FeatureRepository(JanusGraph graph, OFSConfiguration config, ModelRepository modelRepo, PropertyRepository propertyRepository) {
        super(VertexType.FEATURE, graph, propertyRepository, config);
        this.modelRepo = modelRepo;
    }

    /**
     * Creates a feature object in the configured Graph. Features are objects
     * which store information & equate to objects in an Object Oriented sense.
     *
     * @param feature the feature object to input into the graph
     * @return the created feature object's id
     * @throws GraphIntegrityException if the feature's properties do not match
     * the schema associated with the Model
     * @throws Exception generic JanusGraph exception
     */
    public String addFeature(Feature feature) throws GraphIntegrityException, Exception {
        try ( GraphTraversalSource g = getTraversalSource()) {
            try ( Transaction tx = g.tx()) {
                String featureId = (String) addVertex(g, feature.getId()).next().id();
                try ( CloseableResourceLock lock = acquireLock(feature)) {
                    applyFeatureAttributes(g, feature.getModelId(), featureId, feature.getProperties());
                    tx.commit();
                }
                return featureId;
            }
        }
    }

    /**
     * Updates a feature object with new input properties
     *
     * @param featureId identifier of the feature to update
     * @param updateProperties properties to apply on the feature associated to
     * the input featureId
     * @throws Exception generic JanusGraph exception
     */
    public void updateFeature(String featureId, List<Property> updateProperties) throws Exception {
        try ( GraphTraversalSource g = getTraversalSource()) {
            try ( Transaction tx = g.tx()) {
                Feature currFeature = readFeature(featureId);
                try ( CloseableResourceLock lock = acquireLock(currFeature)) {
                    updateFeature(currFeature.getId(), currFeature.getModelId(), updateProperties, g);
                    tx.commit();
                }
            }
        }
    }

    private void updateFeature(String featureId, String modelId, List<Property> updateProperties, GraphTraversalSource g) throws Exception {
        // drop all current edges
        g.V(featureId).outE().drop().iterate();
        // drop all attribtues
        g.V(featureId).properties().drop().iterate();
        // add back on prop for node type
        g.V(featureId).property(OFSRepository.NODE_TYPE_FIELD, VertexType.FEATURE).next();
        // apply props needed
        applyFeatureAttributes(g, modelId, featureId, updateProperties);
    }

    /**
     * Reads the feature object associated to the input id
     *
     * @param id identifier of the Feature to read
     * @param select set of fields to return on the read feature
     * @return Feature object read from the Graph
     * @throws Exception generic JanusGraph exception
     */
    public Feature readFeature(String id, Set<String> select) throws Exception {
        return readFeature(id, Optional.of(select));
    }

    /**
     * Reads the feature object associated to the input id
     *
     * @param id identifier of the Feature to read
     * @return Feature object read from the Graph
     * @throws Exception generic JanusGraph exception
     */
    public Feature readFeature(String id) throws Exception {
        return readFeature(id, PropertyUtil.ALL_SELECT);
    }

    /**
     * Reads the feature object associated to the input id
     *
     * @param id identifier of the Feature to read
     * @param select optional set of fields to return on the read feature. empty
     * optional means all fields.
     * @return Feature object read from the Graph
     * @throws Exception generic JanusGraph exception
     */
    public Feature readFeature(String id, Optional<Set<String>> select) throws Exception {
        try ( GraphTraversalSource g = getTraversalSource()) {
            return readFeature(id, g, select);
        }
    }

    private Feature readFeature(String id, GraphTraversalSource g, Optional<Set<String>> select) throws VertexNotFoundException, Exception {
        return new RepoFeature(readVertexFromGraph(id, g), select, g);
    }

    /**
     * Deletes the feature associated to the input featureId in the Graph
     *
     * @param featureId id of feature to delete
     * @throws GraphIntegrityException if the vertex is being referenced
     * @throws Exception generic JanusGraph exception
     */
    public void deleteFeature(String featureId) throws GraphIntegrityException, Exception {
        deleteVertex(featureId, OFSType.FEATURE);
    }

    /**
     * Reclassify a feature under a different model.This is useful if you're moving features to a new model.
     *
     * @param featureId id if feature you're moving to a new model
     * @param newModelId id of the new model for the feature
     * @throws Exception generic JanusGraph exception
     */
    public void castFeatureToModel(String featureId, String newModelId) throws Exception {
        Feature currFeature = readFeature(featureId);
        castFeatureToModel(featureId, newModelId, currFeature.getProperties());
    }

    /**
     * Reclassify a feature under a different model. This is useful if you're
     * moving features to a new model.
     *
     * @param featureId id if feature you're moving to a new model
     * @param newModelId id of the new model for the feature
     * @param updatedProperties properties you're putting on the feature instead
     * of the old properties
     * @throws Exception generic JanusGraph exception
     */
    public void castFeatureToModel(String featureId, String newModelId, List<Property> updatedProperties) throws Exception {
        // update feature with new props
        try ( GraphTraversalSource g = getTraversalSource()) {
            try ( Transaction tx = g.tx()) {
                try ( CloseableResourceLock lock = acquireLock(OFSType.FEATURE, featureId)) {
                    try ( CloseableResourceLock lock2 = acquireLock(OFSType.MODEL, newModelId)) {
                        // verify feature meets new model schema & get new props
                        Schema fullFeatureSchema = getFullDynamicPropSchema(newModelId);
                        List<Property> inpProps = calcPropsToUse(fullFeatureSchema, updatedProperties);
                        // verify feature is not in use (prevents messing up fetures as properties on other features or models)
                        verifyIntegretyBeforeMutation(g, featureId);
                        // update feature with new props & model
                        updateFeature(featureId, newModelId, inpProps, g);
                        // done, commit
                        tx.commit();
                    }
                }
            }
        }
    }

    private void applyFeatureAttributes(GraphTraversalSource g, String modelId, String featureId, List<Property> properties) throws Exception {
        // calc props needed
        Schema fullFeatureSchema = getFullDynamicPropSchema(modelId);
        List<Property> inpProps = calcPropsToUse(fullFeatureSchema, properties);
        // add attr properties
        addPropsToVertex(inpProps, buildPropInfoMap(fullFeatureSchema), featureId, g);
        // add model id prop & edge
        g.V(featureId).property(OFSConfiguration.MODEL_ID, modelId).next();
        g.V(featureId).addE(OFSConfiguration.REF_USES_MODEL).to(V(modelId)).next();
        // add all models as an inheritsFrom prop
        g.V(featureId).property(INHERITS_FROM_PROP, modelId).next();
        for (String superModelId : getSuperModelIds(modelId, g)) {
            g.V(featureId).property(INHERITS_FROM_PROP, superModelId).next();
        }
    }

    private Schema getFullDynamicPropSchema(String modelId) throws Exception {
        return modelRepo.getRequiredFeatureProperties(modelId);
    }

    private Set<String> getSuperModelIds(String modelId, GraphTraversalSource g) throws Exception {
        Model givenModel = new RepoModel(g.V(modelId).next(), PropertyUtil.EMPTY_SELECT, g);
        Set<String> modelIds = new HashSet<>();
        for (String id : givenModel.getInheritsFromIds()) {
            modelIds.add(id);
            modelIds.addAll(getSuperModelIds(id, g));
        }
        return modelIds;
    }

    @Override
    protected Feature buildType(Vertex v, GraphTraversalSource g, Optional<Set<String>> select) {
        return new RepoFeature(v, select, g);
    }

}
