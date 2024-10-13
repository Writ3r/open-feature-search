/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.index;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import org.janusgraph.core.schema.Parameter;

/**
 *
 * All JanusGraph property types can be serialized using their toString()
 *
 * @author Lucas Wing
 */
public class ParameterSerializer extends StdSerializer<Parameter> {

    protected static final String NAME_KEY = "key";
    protected static final String VAL_KEY = "value";

    public ParameterSerializer() {
        this(null);
    }

    public ParameterSerializer(Class<Parameter> t) {
        super(t);
    }

    @Override
    public void serialize(Parameter param, JsonGenerator gen, SerializerProvider sp) throws IOException {
        gen.writeStartObject();
        gen.writeStringField(NAME_KEY, param.key());
        gen.writeFieldName(VAL_KEY);
        ObjectMapper om = new ObjectMapper();
        // enable default typing since value is a generic obj
        om.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        om.activateDefaultTyping(LaissezFaireSubTypeValidator.instance, DefaultTyping.NON_FINAL_AND_ENUMS);
        // write param value to output
        gen.writeRawValue(om.writeValueAsString(param.value()));
        gen.writeEndObject();
    }

}
