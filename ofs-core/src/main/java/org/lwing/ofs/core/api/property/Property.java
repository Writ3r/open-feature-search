/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.property;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Key/Value property object
 * 
 * @author Lucas Wing
 */
@JsonSerialize(using = PropertySerializer.class)
@JsonDeserialize(using = PropertyDeserializer.class)
public class Property extends PropertyKey {
    
    private final Object value;

    public Property (
           @JsonProperty("name") String name,
           @JsonProperty("value") Object value
    ) {
        super(name);
        this.value = value;
    }
    
    public Property (String name) {
        super(name);
        this.value = null;
    }

    public Object getValue() {
        return value;
    }
   
}
