/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api;

import org.lwing.ofs.core.api.property.Property;
import java.util.List;

/**
 * OFS vertex which contains properties
 * 
 * @author Lucas Wing
 */
public abstract class PropertiesVertex extends OFSVertex {
    
    private final List<Property> properties;
    
    protected PropertiesVertex(String id, List<Property> properties) {
        super(id);
        this.properties = properties;
    }

    public List<Property> getProperties() {
        return properties;
    }
    
}
