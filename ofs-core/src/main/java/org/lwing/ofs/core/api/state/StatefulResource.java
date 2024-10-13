/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.state;

import java.util.Set;

/**
 * OFS object which contains dependency state
 * 
 * @author Lucas Wing
 */
public interface StatefulResource {
    
    public Set<DependencyResource> calcDependencies();
    
    public DependencyResource calcResource();
    
}
