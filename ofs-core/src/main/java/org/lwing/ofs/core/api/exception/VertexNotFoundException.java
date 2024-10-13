/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.exception;

/**
 * Exception thrown if the associated vertex cannot be found
 * @author Lucas Wing
 */
public class VertexNotFoundException extends Exception {
    
    private final String vertexId;

    public VertexNotFoundException(String vertexId) {
        super(String.format("Failed to find vertex [%s]", vertexId));
        this.vertexId = vertexId;
    }

    public String getVertexId() {
        return vertexId;
    }
    
}
