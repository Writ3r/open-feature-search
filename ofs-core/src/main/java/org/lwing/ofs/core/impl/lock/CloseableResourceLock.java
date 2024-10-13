/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.lock;

import org.lwing.ofs.core.api.lock.ResourceLock;
import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

/**
 *
 * @author Lucas Wing
 */
public class CloseableResourceLock implements Closeable {
    
    private final Set<String> resources;
    
    private final ResourceLock resourceLock;
    
    public CloseableResourceLock(ResourceLock resourceLock, Set<String> resources) {
        this.resources = resources;
        this.resourceLock = resourceLock;
    }
    
    public CloseableResourceLock open() {
        resourceLock.acquireLocks(resources);
        return this;
    }

    @Override
    public void close() throws IOException {
        resourceLock.releaseLocks(resources);
    }
    
}
