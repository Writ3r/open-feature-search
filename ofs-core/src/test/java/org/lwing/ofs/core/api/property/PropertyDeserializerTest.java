/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.property;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import org.lwing.ofs.core.api.MappingUtil;
import org.lwing.ofs.core.api.property.PropertyDeserializer.JsonClassNotFoundException;
import org.lwing.ofs.core.api.property.PropertyDeserializer.JsonException;

/**
 *
 * @author Lucas Wing
 */
public class PropertyDeserializerTest {

    @Test
    public void testPropertyDeserializeValidValue() throws JsonProcessingException {
        Property prop = new Property("testKey", 10L);
        String propStr = MappingUtil.turnObjToJsonString(prop);
        ObjectMapper mapper = new ObjectMapper();
        Property out = mapper.readValue(propStr, Property.class);
        assertEquals(prop, out);
    }

    @Test
    public void testPropertyDeserializeInvalidValue() throws JsonProcessingException {
        Property prop = new Property("testKey", new Property("badValue"));
        String propStr = MappingUtil.turnObjToJsonString(prop);
        assertThrows(JsonException.class, () -> {
            ObjectMapper mapper = new ObjectMapper();
            mapper.readValue(propStr, Property.class);
        });
    }

    @Test
    public void testPropertyDeserializeInvalidClass() throws JsonProcessingException {
        Property prop = new Property("testKey", 10);
        String propStr = MappingUtil.turnObjToJsonString(prop).replace("Integer", "BadClass");
        assertThrows(JsonClassNotFoundException.class, () -> {
            ObjectMapper mapper = new ObjectMapper();
            mapper.readValue(propStr, Property.class);
        });
    }

}
