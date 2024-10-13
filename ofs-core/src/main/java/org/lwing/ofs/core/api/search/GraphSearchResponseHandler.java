/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.search;

import java.util.Iterator;
import org.lwing.ofs.core.api.OFSIdVertex;

/**
 * Response handler for JanusGraph searches to help transform
 * the results into OFS objects
 * 
 * @author Lucas Wing
 */
@FunctionalInterface
public interface GraphSearchResponseHandler<E extends OFSIdVertex> {
    
    public void handleResponse(Iterator<E> nodeIterator);
    
}
