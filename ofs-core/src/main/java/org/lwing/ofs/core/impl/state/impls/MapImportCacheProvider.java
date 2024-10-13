/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.state.impls;

import java.util.HashSet;
import java.util.Set;
import org.lwing.ofs.core.api.state.ImportCacheProvider;

/**
 *
 * @author Lucas Wing
 */
public class MapImportCacheProvider implements ImportCacheProvider {

    @Override
    public ImportCache getImportCache() {
        return new ImportCacheImpl();
    }

    @Override
    public void deleteImportCache(ImportCache cache) {
        // do nothing, java will cleanup unused obj
    }
    
    class ImportCacheImpl implements ImportCache {
        
        private Set<String> resourceSet;

        public ImportCacheImpl() {
            this.resourceSet = new HashSet<>();
        }
        
        @Override
        public void storeResource(String resource) {
            resourceSet.add(resource);
        }

        @Override
        public boolean containsResource(String resource) {
            return resourceSet.contains(resource);
        }
        
    }
    
}
