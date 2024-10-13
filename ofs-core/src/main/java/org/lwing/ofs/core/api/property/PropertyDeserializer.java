/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.property;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.UUID;
import org.janusgraph.core.attribute.Geoshape;
import static org.lwing.ofs.core.api.property.PropertySerializer.CLASS_KEY;
import static org.lwing.ofs.core.api.property.PropertySerializer.NAME_KEY;
import static org.lwing.ofs.core.api.property.PropertySerializer.VAL_KEY;

/**
 * https://www.baeldung.com/jackson-deserialization
 *
 * @author Lucas Wing
 */
public class PropertyDeserializer extends StdDeserializer<Property> {

    public PropertyDeserializer() {
        this(null);
    }

    public PropertyDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public Property deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        String name = null;
        try {
            JsonNode node = jp.getCodec().readTree(jp);
            name = node.get(NAME_KEY).textValue();
            Class<?> clazz = Class.forName(node.get(CLASS_KEY).textValue());
            return new Property(name, getValue(node.get(VAL_KEY), clazz));
        } catch (ClassNotFoundException ex) {
            throw new JsonClassNotFoundException("Failed to deserialize obj because of classNotFound", ex);
        } catch (ParseException ex) {
            throw new JsonException("Failed to deserialize Geoshape obj on property: " + name);
        }
    }

    private Object getValue(JsonNode input, Class<?> clazz) throws JsonException, ParseException {
        Object output;
        if (String.class.isAssignableFrom(clazz)) {
            output = input.asText();
        } else if (Character.class.isAssignableFrom(clazz)) {
            output = input.asText().charAt(0);
        } else if (Boolean.class.isAssignableFrom(clazz)) {
            output = input.asBoolean();
        } else if (Byte.class.isAssignableFrom(clazz)) {
            output = Byte.parseByte(input.textValue());
        } else if (Short.class.isAssignableFrom(clazz)) {
            output = Short.parseShort(input.asText());
        } else if (Integer.class.isAssignableFrom(clazz)) {
            output = input.asInt();
        } else if (Long.class.isAssignableFrom(clazz)) {
            output = input.asLong();
        } else if (Float.class.isAssignableFrom(clazz)) {
            output = Float.parseFloat(input.asText());
        } else if (Double.class.isAssignableFrom(clazz)) {
            output = input.asDouble();
        } else if (Date.class.isAssignableFrom(clazz)) {
            output = new Date(input.asLong());
        } else if (Geoshape.class.isAssignableFrom(clazz)) {
            output = Geoshape.fromWkt(input.asText());
        } else if (UUID.class.isAssignableFrom(clazz)) {
           output = UUID.fromString(input.asText());
        } else {
            throw new JsonException("Cannot find class to deserialize property value " + clazz.getCanonicalName());
        }
        return output;
    }

    class JsonClassNotFoundException extends JsonProcessingException {

        public JsonClassNotFoundException(String cause, Throwable ex) {
            super(cause, ex);
        }

    }

    class JsonException extends JsonProcessingException {

        public JsonException(String cause) {
            super(cause);
        }

    }
}
