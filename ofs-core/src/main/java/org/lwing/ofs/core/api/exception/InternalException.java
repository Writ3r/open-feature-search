/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.exception;

/**
 *
 * @author Lucas Wing
 */
public class InternalException extends Exception{ 
    
    public InternalException(Exception ex) {
        super(ex);
    }
    
    public InternalException(String message, Object... messageArgs) {
        super(String.format(message, messageArgs));
    }
    
}
