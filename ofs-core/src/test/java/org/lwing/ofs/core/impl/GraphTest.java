/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.lwing.ofs.core.impl.OFSRepository;
import org.lwing.ofs.core.api.JGMgntProvider;
import org.lwing.ofs.core.api.config.OFSConfigurationParams;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.exception.InternalKeywordException;
import org.lwing.ofs.core.api.feature.Feature;
import org.lwing.ofs.core.api.model.Model;
import org.lwing.ofs.core.api.property.Cardinality;
import org.lwing.ofs.core.api.property.PrimitivePropertyKey;
import org.lwing.ofs.core.api.property.Property;
import org.lwing.ofs.core.api.schema.Schema;
import org.lwing.ofs.core.impl.property.PropertyRepository;
import org.lwing.ofs.core.impl.model.ModelSchemaRepository;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphFactory.Builder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.lwing.ofs.core.OpenFeatureStore;
import org.lwing.ofs.core.api.schema.FeatureSchema;
import org.lwing.ofs.core.api.schema.ModelSchema;
import org.mockito.Mockito;

/**
 * TODO: clean this class up later, it's a mess lol
 *
 * @author Lucas Wing
 */
public abstract class GraphTest {

    protected final String BASIC_MODEL_PROP_NAME = "testModelprop";

    protected OpenFeatureStore openFeatureStore;

    protected JanusGraph graph;

    @BeforeEach
    public void setupTests() throws GraphIntegrityException, InterruptedException, ExecutionException, InternalKeywordException, IOException {
        setupTests(false);
    }
    
    protected void setupTests(boolean withLucene) throws IOException, GraphIntegrityException, 
            InterruptedException, ExecutionException, InternalKeywordException {
        setupOpenFeatureStore(withLucene);
        openFeatureStore.getOpenFeatureStoreInstaller().install(Collections.EMPTY_SET);
    }

    protected void setupOpenFeatureStore(boolean withLucene) throws IOException {
        Builder basicBuilder = basicJanusgraphBuilder();
        OFSConfigurationParams params = OFSConfigurationParams.build();
        if (withLucene) {
            wipeLuceneDirIfExists();
            Files.createDirectory(getLuceneDir());
            basicBuilder.set("index.search.backend", "lucene") // sets up lucene
                    .set("index.search.directory", getLuceneDir().toString()).open(); // sets data dir
            params.setMixedIndexName("search");
        }
        graph = basicBuilder.open(); // allows string ids
        openFeatureStore = getOpenFeatureStore(params);
    }

    private Builder basicJanusgraphBuilder() {
        return JanusGraphFactory.build()
                .set("storage.backend", "inmemory") // in-memory for testing
                .set("schema.default", "none") // forces user to define schema
                .set("graph.set-vertex-id", true) // allows custom ids
                .set("graph.allow-custom-vid-types", true);
    }

    @AfterEach
    public void teardownTests() throws IOException {
        graph.close();
        wipeLuceneDirIfExists();
    }

    protected OpenFeatureStore getOpenFeatureStore() {
        return getOpenFeatureStore(OFSConfigurationParams.build());
    }

    protected OpenFeatureStore getOpenFeatureStore(OFSConfigurationParams params) {
        return new OpenFeatureStore(graph, params);
    }

    // a bit hacky, but needed to work around useManagement protected access
    protected void configureMockMgntProvider(OFSRepository repo, JGMgntProvider provider) {
        Mockito.doReturn(provider).when(repo).useManagement();
    }

    protected Model createBasicModel(OpenFeatureStore search) throws Exception {
        return createBasicModel(search, true);
    }

    protected Model createBasicModel(OpenFeatureStore search, boolean createProps) throws Exception {
        if (createProps) {
            // create feature props
            PropertyRepository repo = search.getPropertyRepository();
            PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
            repo.createProperty(testProp);
        }
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop", "test"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        return createBasicModel(search, newFeatureSchema, createProps);
    }

    protected Model createBasicModel(OpenFeatureStore search, FeatureSchema newFeatureSchema) throws Exception {
        return createBasicModel(search, newFeatureSchema, true);
    }

    protected Model createBasicModel(OpenFeatureStore search, FeatureSchema newFeatureSchema, boolean createProps) throws Exception {
        return createBasicModel(search, newFeatureSchema, new HashSet<>(), createProps);
    }

    protected Model createBasicModel(OpenFeatureStore search, FeatureSchema newFeatureSchema, Set<String> inheritsFromIds, boolean createProps) throws Exception {
        // create model schema
        Schema outModelSchema = makeBasicModelSchema(search, createProps);
        // create model
        List<Property> modelProps = new ArrayList<>();
        modelProps.add(new Property(BASIC_MODEL_PROP_NAME, "test"));
        Model inpModel = new Model(inheritsFromIds, modelProps, outModelSchema.getId(), newFeatureSchema);
        String modelId = search.getModelRepository().createModel(inpModel);
        return search.getModelRepository().readModel(modelId);
    }

    protected Schema makeBasicModelSchema(OpenFeatureStore search) throws Exception {
        return makeBasicModelSchema(search, true);
    }

    protected Schema makeBasicModelSchema(OpenFeatureStore search, boolean createProps) throws Exception {
        if (createProps) {
            // create props
            PrimitivePropertyKey testModelProp = new PrimitivePropertyKey(BASIC_MODEL_PROP_NAME, String.class, Cardinality.SINGLE);
            PropertyRepository repo = search.getPropertyRepository();
            repo.createProperty(testModelProp);
        }
        // make schema
        ModelSchemaRepository modelSchemaRepo = search.getModelSchemaRepository();
        ModelSchema newSchema = new ModelSchema(new HashSet<>(Arrays.asList(BASIC_MODEL_PROP_NAME)));
        String schemaId = modelSchemaRepo.createSchema(newSchema);
        return modelSchemaRepo.readSchema(schemaId);
    }

    protected Feature createBasicFeature(OpenFeatureStore search, Model newModelRet) throws GraphIntegrityException, Exception {
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testprop", "test"));
        Feature inpFeature = new Feature(newModelRet.getId(), featureProps);
        String featureId = search.getFeatureRepository().addFeature(inpFeature);;
        return search.getFeatureRepository().readFeature(featureId);
    }

    protected Feature createBasicFeature(OpenFeatureStore search) throws GraphIntegrityException, Exception {
        Model newModelRet = createBasicModel(search);
        return createBasicFeature(search, newModelRet);
    }

    protected Set<String> getPropKeysFromProps(List<Property> properties) {
        return properties.stream().map(f -> f.getName()).collect(Collectors.toSet());
    }

    private void wipeLuceneDirIfExists() throws IOException {
        FileUtils.deleteDirectory(getLuceneDir().toFile());
    }

    private Path getLuceneDir() {
        return Paths.get("target/lucene").toAbsolutePath();
    }

}
