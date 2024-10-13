/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.lock;

import java.util.Set;

/**
 *
 * @author Lucas Wing
 */
public interface ResourceLock {
    
    /**
     * Acquires locks for a set of resources
     * @param resources resources to lock
     */
    public void acquireLocks(Set<String> resources);
    
    /**
     * Releases locks for a set of resources
     * @param resources resources to release
     */
    public void releaseLocks(Set<String> resources);
    
}
