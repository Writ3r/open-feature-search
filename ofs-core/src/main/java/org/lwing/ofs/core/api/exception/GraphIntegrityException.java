/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.exception;

/**
 * Exception which occurs if an attempt is made to violate the integrity of the OFS Graph.
 * An example of this would be attempting to delete Models which Features reference, or not setting
 * required properties on a Feature by the associated Model.
 * 
 * @author Lucas Wing
 */
public class GraphIntegrityException extends Exception {
    
    public GraphIntegrityException(String message, Object... messageArgs) {
        super(String.format(message, messageArgs));
    }
    
}
