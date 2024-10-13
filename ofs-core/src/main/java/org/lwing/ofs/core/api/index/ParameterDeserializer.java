/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.index;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import java.io.IOException;
import org.janusgraph.core.schema.Parameter;
import static org.lwing.ofs.core.api.index.ParameterSerializer.NAME_KEY;
import static org.lwing.ofs.core.api.index.ParameterSerializer.VAL_KEY;

/**
 * https://www.baeldung.com/jackson-deserialization
 *
 * @author Lucas Wing
 */
public class ParameterDeserializer extends StdDeserializer<Parameter> {

    public ParameterDeserializer() {
        this(null);
    }

    public ParameterDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public Parameter deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper om = new ObjectMapper();
        // enable default typing since value is a generic obj
        om.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        om.activateDefaultTyping(LaissezFaireSubTypeValidator.instance, ObjectMapper.DefaultTyping.NON_FINAL_AND_ENUMS);
        // Parse the JSON for Parameter fields and deserialize it
        JsonNode node = jp.getCodec().readTree(jp);
        String key = node.get(NAME_KEY).asText();
        String value = node.get(VAL_KEY).toString();
        Object valueObj = om.readValue(value, Object.class);
        return new Parameter(key, valueObj);
    }

}
