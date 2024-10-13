/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api;

import java.io.Closeable;
import org.janusgraph.core.schema.JanusGraphManagement;

/**
 * If management is left open without a commit, some operations like making indexes get stuck..
 * Using this will force commits after opening the provider.
 * 
 * @author Lucas Wing
 */
public class JGMgntProvider implements Closeable {
    
    private final JanusGraphManagement mgnt;
    
    public JGMgntProvider(JanusGraphManagement mgnt) {
        this.mgnt = mgnt;
    }

    @Override
    public void close() {
        if (mgnt.isOpen()) {
            mgnt.commit();
        }
    }

    public JanusGraphManagement getMgnt() {
        return mgnt;
    }
    
}
