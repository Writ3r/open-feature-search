/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.index;

/**
 * The types of supported indices, these should match up with JanusGraph.
 * This is here in case there's different element types JanusGraph has/adds which OFS does not support.
 * 
 * @author Lucas Wing
 */
public enum IndexType {
    COMPOSITE,
    MIXED
}
