/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core;

import org.lwing.ofs.core.api.config.OFSConfiguration;
import org.lwing.ofs.core.api.config.OFSConfigurationParams;
import org.lwing.ofs.core.impl.feature.FeatureRepository;
import org.lwing.ofs.core.impl.index.IndexRepository;
import org.lwing.ofs.core.impl.install.OFSInstaller;
import org.lwing.ofs.core.impl.model.ModelRepository;
import org.lwing.ofs.core.impl.property.PropertyRepository;
import org.lwing.ofs.core.impl.model.FeatureSchemaRepository;
import org.lwing.ofs.core.impl.model.ModelSchemaRepository;
import org.lwing.ofs.core.impl.view.ViewSchemaRepository;
import org.lwing.ofs.core.impl.view.ViewRepository;
import org.janusgraph.core.JanusGraph;
import org.lwing.ofs.core.impl.state.StateManager;

/**
 *
 * @author Lucas Wing
 */
public class OpenFeatureStore {

    private final FeatureRepository featureRepository;

    private final ModelRepository modelRepository;

    private final ViewRepository viewRepository;

    private final PropertyRepository propertyRepository;

    private final IndexRepository indexRepository;

    private final ModelSchemaRepository modelSchemaRepository;

    private final FeatureSchemaRepository featureSchemaRepository;

    private final ViewSchemaRepository viewSchemaRepository;

    private final OFSConfiguration config;

    private final OFSInstaller openFeatureStoreInstaller;
    
    private final StateManager stateManager;

    /**
     * Creates the OpenFeatureStore object repositories. Use this object to get the
     * repositories for all of your operations.
     *
     * @param graph JanusGraph object with access to a graph traversal source
     * and management API
     * @param params OpenFeatureStore creation parameters
     */
    public OpenFeatureStore(JanusGraph graph, OFSConfigurationParams params) {
        this.config = new OFSConfiguration(params);
        this.propertyRepository = new PropertyRepository(graph, config);
        this.indexRepository = new IndexRepository(graph, config);
        this.modelSchemaRepository = new ModelSchemaRepository(graph, propertyRepository, config);
        this.featureSchemaRepository = new FeatureSchemaRepository(graph, propertyRepository, config);
        this.viewSchemaRepository = new ViewSchemaRepository(graph, propertyRepository, config);
        this.modelRepository = new ModelRepository(graph, modelSchemaRepository, featureSchemaRepository, config, propertyRepository);
        this.featureRepository = new FeatureRepository(graph, config, modelRepository, propertyRepository);
        this.viewRepository = new ViewRepository(graph, config, propertyRepository, viewSchemaRepository, modelRepository);
        this.openFeatureStoreInstaller = new OFSInstaller(graph, config, indexRepository);
        this.stateManager = new StateManager(indexRepository, propertyRepository, featureRepository, 
                modelRepository, viewRepository, modelSchemaRepository, viewSchemaRepository, config);
    }

    public FeatureRepository getFeatureRepository() {
        return featureRepository;
    }

    public ModelRepository getModelRepository() {
        return modelRepository;
    }

    public ViewRepository getViewRepository() {
        return viewRepository;
    }

    public PropertyRepository getPropertyRepository() {
        return propertyRepository;
    }

    public IndexRepository getIndexRepository() {
        return indexRepository;
    }

    public ModelSchemaRepository getModelSchemaRepository() {
        return modelSchemaRepository;
    }

    public FeatureSchemaRepository getFeatureSchemaRepository() {
        return featureSchemaRepository;
    }

    public ViewSchemaRepository getViewSchemaRepository() {
        return viewSchemaRepository;
    }

    public OFSConfiguration getConfig() {
        return config;
    }

    public OFSInstaller getOpenFeatureStoreInstaller() {
        return openFeatureStoreInstaller;
    }

    public StateManager getStateManager() {
        return stateManager;
    }

}
