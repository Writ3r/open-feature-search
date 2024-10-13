/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.janusgraph.core.schema.Parameter;
import org.lwing.ofs.core.api.index.ParameterDeserializer;
import org.lwing.ofs.core.api.index.ParameterSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Lucas Wing
 */
public class MappingUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(MappingUtil.class);

    private MappingUtil() {

    }
    
    public static <E> E jsonStringToObject(String input, Class<E> output) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addDeserializer(Parameter.class,new ParameterDeserializer());
        mapper.registerModule(simpleModule);
        return mapper.readValue(input, output);
    }

    public static String turnObjToJsonString(Object obj) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(Parameter.class, new ParameterSerializer());
        mapper.registerModule(simpleModule);
        return mapper.writeValueAsString(obj);
    }

}
