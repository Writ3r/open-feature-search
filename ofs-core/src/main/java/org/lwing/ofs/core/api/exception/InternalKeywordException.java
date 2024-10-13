/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.exception;

import org.lwing.ofs.core.api.config.OFSConfiguration;

/**
 * Exception which occurs on use of a field considered internal to OFS.
 * OFS needs certain fields to operate which a user cannot interact with.
 * 
 * @author Lucas Wing
 */
public class InternalKeywordException extends Exception {
    
    private final String field;

    public InternalKeywordException(String field) {
        super(String.format("Input field [%s] attempted to use an internal prefix [%s]", 
                field, OFSConfiguration.INTERNAL_FIELD_PREFIX));
        this.field = field;
    }

    public String getField() {
        return field;
    }
    
}
