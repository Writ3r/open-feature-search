/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl;

import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.feature.Feature;
import org.lwing.ofs.core.api.model.Model;
import org.lwing.ofs.core.impl.feature.RepoFeature;
import org.lwing.ofs.core.impl.model.RepoModel;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 *
 * @author Lucas Wing
 */
public class TypeVerifier {

    private TypeVerifier() {
    }

    /**
     * Verifies a feature fits the model specified by a reference
     * property. Features can fit the model if they extend that model, or the
     * model they extend inherits from the model specified.
     *
     * @param refPropId id of the ref prop to check against the model
     * @param g traversal source
     * @param modelId id of the model
     *
     * @throws GraphIntegrityException thrown if the property doesn't exist or
     * the feature does not inherit from the model
     * @throws Exception generic JanusGraph search exception
     */
    public static void verifyFeatureFitsProp(String refPropId, GraphTraversalSource g, String modelId) throws GraphIntegrityException, Exception {
        Model propModel = new RepoModel(g.V(modelId).next(), PropertyUtil.EMPTY_SELECT, g);
        Feature feature = new RepoFeature(g.V(refPropId).next(), PropertyUtil.EMPTY_SELECT, g);
        if (!isFeatureSubtype(feature, propModel, g)) {
            throw new GraphIntegrityException("Feature with id [%s] is not a subtype of a model with id %s", feature.getId(), propModel.getId());
        }
    }

    /**
     * Verifies a feature fits the model specified. Features can fit the model
     * if they extend that model, or the model they extend inherits from the
     * model specified.
     *
     * @param givenFeature feature getting checked
     * @param superModel model which should be a super type of the feature
     * @param g traversal source
     * @return if the feature is a subtype of the model
     * @throws Exception generic JanusGraph search exception
     */
    public static boolean isFeatureSubtype(Feature givenFeature, Model superModel, GraphTraversalSource g) throws Exception {
        Vertex v = g.V(givenFeature.getModelId()).next();
        return isModelSubtype(new RepoModel(v, PropertyUtil.EMPTY_SELECT, g), superModel, g);
    }

    private static boolean isModelSubtype(Model givenModel, Model superModel, GraphTraversalSource g) throws Exception {
        if (givenModel.equals(superModel)) {
            return true;
        }
        for (String id : givenModel.getInheritsFromIds()) {
            Model parent = new RepoModel(g.V(id).next(), PropertyUtil.EMPTY_SELECT, g);
            if (isModelSubtype(parent, superModel, g)) {
                return true;
            }
        }
        return false;
    }

}
