/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.state;

/**
 * Cache used during import to keep track of what resources have already been imported.
 * This is used because some graphs might have long commit times so checking the graph
 * may not be sufficient.
 * 
 * @author Lucas Wing
 */
public interface ImportCacheProvider {

    public ImportCache getImportCache();

    public void deleteImportCache(ImportCache cache);

    public interface ImportCache {

        public void storeResource(String resource);

        public boolean containsResource(String resource);
    }

}
