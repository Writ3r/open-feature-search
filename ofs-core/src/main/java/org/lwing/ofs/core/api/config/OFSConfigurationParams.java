/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.config;

import org.lwing.ofs.core.api.lock.ResourceLock;
import org.lwing.ofs.core.api.state.ImportCacheProvider;
import org.lwing.ofs.core.impl.lock.ResourceLockImpl;
import org.lwing.ofs.core.impl.state.impls.GraphFilesystemStorage;
import org.lwing.ofs.core.api.state.GraphStorageSystem;
import org.lwing.ofs.core.impl.state.impls.MapImportCacheProvider;

/**
 * Parameter inputs to OFS core
 * 
 * @author Lucas Wing
 */
public class OFSConfigurationParams {
    
    // name of a mixed index backend to use for JanusGraph
    private String mixedIndexName;

    // lock used when executing actions on certain resources
    private ResourceLock resourceLock;
    
    // used during import and export to access storage for exports to a Path
    private GraphStorageSystem graphStorageProvider;
    
    // stores which resources have been imported during a state import
    private ImportCacheProvider importCacheProvider;

    public OFSConfigurationParams() {
        this.resourceLock = new ResourceLockImpl();
        this.graphStorageProvider = new GraphFilesystemStorage();
        this.importCacheProvider = new MapImportCacheProvider();
        this.mixedIndexName = null;
    }
    
    public static OFSConfigurationParams build() {
        return new OFSConfigurationParams();
    }

    public String getMixedIndexName() {
        return mixedIndexName;
    }

    public OFSConfigurationParams setMixedIndexName(String mixedIndexName) {
        this.mixedIndexName = mixedIndexName;
        return this;
    }

    public ResourceLock getResourceLock() {
        return resourceLock;
    }

    public OFSConfigurationParams setResourceLock(ResourceLock resourceLock) {
        this.resourceLock = resourceLock;
        return this;
    }

    public GraphStorageSystem getGraphStorageProvider() {
        return graphStorageProvider;
    }

    public OFSConfigurationParams setGraphStorageProvider(GraphStorageSystem graphStorageProvider) {
        this.graphStorageProvider = graphStorageProvider;
        return this;
    }

    public ImportCacheProvider getImportCacheProvider() {
        return importCacheProvider;
    }

    public OFSConfigurationParams setImportCacheProvider(ImportCacheProvider importCacheProvider) {
        this.importCacheProvider = importCacheProvider;
        return this;
    }
    
}
