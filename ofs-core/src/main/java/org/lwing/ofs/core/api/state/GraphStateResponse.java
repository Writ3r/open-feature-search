/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.state;

import java.util.Set;

/**
 *
 * @author Lucas Wing
 */
public class GraphStateResponse {
    
    Set<String> failedResources;

    public GraphStateResponse(Set<String> failedResources) {
        this.failedResources = failedResources;
    }

    public Set<String> getFailedResources() {
        return failedResources;
    }
    
}
