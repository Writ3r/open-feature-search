/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.lwing.ofs.core.api.lock.ResourceLock;
import org.lwing.ofs.core.api.state.GraphStorageSystem;
import org.lwing.ofs.core.api.state.ImportCacheProvider;
import org.lwing.ofs.core.impl.lock.ResourceLockImpl;
import org.lwing.ofs.core.impl.state.impls.GraphFilesystemStorage;
import org.lwing.ofs.core.impl.state.impls.MapImportCacheProvider;

/**
 *
 * @author Lucas Wing
 */
public class OFSConfigurationTest {
    
    @Test
    public void testCfgParamsPassThrough() {
        // make obj inputs
        ResourceLock lock = new ResourceLockImpl();
        ImportCacheProvider importCacheProvider = new MapImportCacheProvider();
        GraphStorageSystem graphStorageProvider = new GraphFilesystemStorage();
        OFSConfigurationParams params = OFSConfigurationParams.build()
                .setMixedIndexName("testName").setResourceLock(lock)
                .setImportCacheProvider(importCacheProvider).setGraphStorageProvider(graphStorageProvider);
        // build cfg
        OFSConfiguration config = new OFSConfiguration(params);
        // verify ref equals to make sure the objs I set are the ones that exist
        assertTrue(lock == config.getResourceLock());
        assertTrue(importCacheProvider == config.getImportCacheProvider());
        assertTrue(graphStorageProvider == config.getGraphStorageProvider());
        assertEquals("testName", config.getMixedIndexName());
    }
    
    @Test
    public void testDefaultConstSetsAllObjs() {
        // build cfg
        OFSConfiguration config = new OFSConfiguration();
        // verify ref equals to make sure the objs I set are the ones that exist
        assertTrue(config.getResourceLock() != null);
        assertTrue(config.getImportCacheProvider() != null);
        assertTrue(config.getGraphStorageProvider() != null);
        assertTrue(config.getMixedIndexName() == null);
    }
    
}
