/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.property;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.util.Date;

/**
 * 
 * All JanusGraph property types can be serialized using their toString()
 *
 * @author Lucas Wing
 */
public class PropertySerializer extends StdSerializer<Property> {
    
    protected static final String NAME_KEY = "name";
    protected static final String VAL_KEY = "value";
    protected static final String CLASS_KEY = "class";
    

    public PropertySerializer() {
        this(null);
    }

    public PropertySerializer(Class<Property> t) {
        super(t);
    }

    @Override
    public void serialize(Property prop, JsonGenerator jgen, SerializerProvider sp) throws IOException {
        jgen.writeStartObject();
        jgen.writeStringField(NAME_KEY, prop.getName());
        jgen.writeStringField(VAL_KEY, getNodeVal(prop.getValue()));
        jgen.writeStringField(CLASS_KEY, (String) prop.getValue().getClass().getCanonicalName());
        jgen.writeEndObject();
    }
    
    private String getNodeVal(Object value) {
        // most types work with toString (except date vals, so using a unix timestamp for these)
        if (value instanceof Date) {
            return String.valueOf(((Date) value).getTime());
        }
        return value.toString();
    }

}
