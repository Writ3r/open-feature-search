/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api;

import java.util.Objects;
import org.lwing.ofs.core.api.state.StatefulResource;

/**
 * OFS object which is stored in JanusGraph as a Vertex
 * 
 * @author Lucas Wing
 */
public abstract class OFSVertex implements OFSIdVertex, StatefulResource {
    
    private final String id;

    protected OFSVertex(String id) {
        this.id = id;
    }

    @Override
    public int hashCode() {
        return this.id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof OFSVertex)) {
            return false;
        }
        final OFSVertex other = (OFSVertex) obj;
        return Objects.equals(this.id, other.id);
    }
    
    @Override
    public String getId() {
        return id;
    }
    
}
