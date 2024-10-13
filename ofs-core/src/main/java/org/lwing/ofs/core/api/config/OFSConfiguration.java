/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.config;

import org.lwing.ofs.core.api.lock.ResourceLock;
import org.lwing.ofs.core.api.state.ImportCacheProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.lwing.ofs.core.api.state.GraphStorageSystem;

/**
 * Configuration store for OFS
 * 
 * @author Lucas Wing
 */
public class OFSConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(OFSConfiguration.class);

    // prefix used to specify a field is internal to OFS
    public static final String INTERNAL_FIELD_PREFIX = "_";

    // used specifically for features, if they 'implement' a model
    public static final String IMPL_EDGE_ID = INTERNAL_FIELD_PREFIX + "implements";

    // field on a feature or ref property specifying the associated model's id
    public static final String MODEL_ID = INTERNAL_FIELD_PREFIX + "model_id";

    // edge label at model for ref properties
    public static final String REF_USES_MODEL = INTERNAL_FIELD_PREFIX + "uses_model";

    // name of a mixed index backend to use for JanusGraph
    private final String mixedIndexName;

    // lock system used for OFS resources
    private final ResourceLock resourceLock;
    
    // allows for modifying internal fields
    private boolean allowInternalFieldActions;
    
    // storage provider for OFS state exports and imports
    private final GraphStorageSystem graphStorageProvider;
    
    // stores which resources have been imported during a state import
    private final ImportCacheProvider importCacheProvider;

    public OFSConfiguration(OFSConfigurationParams params) {
        this.mixedIndexName = params.getMixedIndexName();
        this.resourceLock = params.getResourceLock();
        this.allowInternalFieldActions = false;
        this.graphStorageProvider = params.getGraphStorageProvider();
        this.importCacheProvider = params.getImportCacheProvider();
    }
    
    public OFSConfiguration() {
        OFSConfigurationParams params = new OFSConfigurationParams();
        this.mixedIndexName = params.getMixedIndexName();
        this.resourceLock = params.getResourceLock();
        this.allowInternalFieldActions = false;
        this.graphStorageProvider = params.getGraphStorageProvider();
        this.importCacheProvider = params.getImportCacheProvider();
    }

    public String getMixedIndexName() {
        return mixedIndexName;
    }

    public boolean isAllowInternalFieldActions() {
        return allowInternalFieldActions;
    }

    public ResourceLock getResourceLock() {
        return resourceLock;
    }

    public GraphStorageSystem getGraphStorageProvider() {
        return graphStorageProvider;
    }

    public ImportCacheProvider getImportCacheProvider() {
        return importCacheProvider;
    }

    /**
     * ONLY THE INSTALLER CLASS SHOULD FLIP THIS TO TRUE DURING INSTALLATION
     *
     * @param allowInternalFieldActions if internal edits should be allowed or
     * not
     */
    public void setAllowInternalFieldActions(boolean allowInternalFieldActions) {
        LOGGER.info("Setting allowInternalFieldActions to [{}]", allowInternalFieldActions);
        this.allowInternalFieldActions = allowInternalFieldActions;
    }

}
