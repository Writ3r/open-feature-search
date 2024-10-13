/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.property;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.lwing.ofs.core.api.MappingUtil;
import java.util.Objects;

/**
 * Abstract class that other property key types extend. All property keys should
 * have a unique name so they extend this.
 *
 * @author Lucas Wing
 */
public abstract class PropertyKey {

    private final String name;

    protected PropertyKey(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
        return this.name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final PropertyKey other = (PropertyKey) obj;
        return Objects.equals(this.name, other.name);
    }

    @Override
    public String toString() {
        try {
            return MappingUtil.turnObjToJsonString(this);
        } catch (JsonProcessingException ex) {
            return null;
        }
    }

}
