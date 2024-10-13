/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.state;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.commons.io.FileUtils;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.exception.InternalException;
import org.lwing.ofs.core.api.exception.InternalKeywordException;
import org.lwing.ofs.core.api.feature.Feature;
import org.lwing.ofs.core.api.index.Index;
import org.lwing.ofs.core.api.index.IndexElementType;
import org.lwing.ofs.core.api.index.IndexType;
import org.lwing.ofs.core.api.model.Model;
import org.lwing.ofs.core.api.property.Cardinality;
import org.lwing.ofs.core.api.property.PrimitivePropertyKey;
import org.lwing.ofs.core.api.property.Property;
import org.lwing.ofs.core.api.property.RefPropertyKey;
import org.lwing.ofs.core.api.schema.FeatureSchema;
import org.lwing.ofs.core.api.schema.ModelSchema;
import org.lwing.ofs.core.api.schema.ViewSchema;
import org.lwing.ofs.core.api.view.View;
import org.lwing.ofs.core.impl.GraphTest;
import org.lwing.ofs.core.impl.feature.FeatureITest;
import org.lwing.ofs.core.impl.index.IndexTest;
import static org.lwing.ofs.core.impl.index.IndexTest.buildIndexMap;
import org.lwing.ofs.core.impl.model.ModelSchemaRepository;
import org.lwing.ofs.core.impl.model.ModelTest;
import org.lwing.ofs.core.impl.property.PropertyTest;
import org.lwing.ofs.core.impl.schema.SchemaTest;
import org.lwing.ofs.core.impl.view.ViewITest;

/**
 *
 * @author Lucas Wing
 */
public class StateManagerTest extends GraphTest {

    @BeforeEach
    public void setupExportTests() throws IOException {
        // del export dir if exists
        FileUtils.deleteDirectory(getExportDir().toFile());
        // make export dir
        Files.createDirectory(getExportDir());
    }

    @Test
    public void testBasicExportImport() throws InterruptedException, InternalException, IOException, Exception {
        // cleanup graph so I can remake with mixed index
        teardownTests();
        setupTests(true);
        // add test data
        List<PrimitivePropertyKey> primProps = createTestPrimProps();
        List<Index> indices = createTestIndices();
        List<ModelSchema> mschemas = createTestModelSchema();
        List<Model> models = createTestModel(mschemas.get(0).getId());
        List<RefPropertyKey> refProps = createTestRefProps(models.get(0).getId());
        List<Feature> features = createTestFeatures(models.get(0).getId());
        List<ViewSchema> viewSchemas = createTestViewSchemas();
        List<View> views = createTestViews(viewSchemas.get(0).getId(), models.get(0).getId());
        // exec export
        StateManager manager = openFeatureStore.getStateManager();
        assertEquals(0, manager.exportOFSState(getExportDir()).getFailedResources().size());
        // cleanup graph
        teardownTests();
        setupTests(true);
        // exec import
        manager = openFeatureStore.getStateManager();
        assertEquals(0, manager.importOFSState(getExportDir()).getFailedResources().size());
        // verify all data was imported into the graph
        for (PrimitivePropertyKey obj : primProps) {
            PropertyTest.verifyPropEquality(obj, openFeatureStore.getPropertyRepository().readPrimitiveProperty(obj.getName()).get());
        }
        for (ModelSchema obj : mschemas) {
            SchemaTest.verifySchemaEquals(obj, openFeatureStore.getModelSchemaRepository().readSchema(obj.getId()));
        }
        for (Model obj : models) {
            ModelTest.verifyModelEquals(obj, openFeatureStore.getModelRepository().readModel(obj.getId()));
        }
        for (Feature obj : features) {
            FeatureITest.verifyFeatureEquals(obj, openFeatureStore.getFeatureRepository().readFeature(obj.getId()));
        }
        for (ViewSchema obj : viewSchemas) {
            SchemaTest.verifySchemaEquals(obj, openFeatureStore.getViewSchemaRepository().readSchema(obj.getId()));
        }
        for (View obj : views) {
            ViewITest.verifyViewEquals(obj, openFeatureStore.getViewRepository().readView(obj.getId()));
        }
        for (RefPropertyKey obj : refProps) {
            PropertyTest.verifyPropEquality(obj, openFeatureStore.getPropertyRepository().readRefProperty(obj.getName()).get());
        }
        for (Index obj : indices) {
            IndexTest.verifyIndexFieldsMatch(obj, openFeatureStore.getIndexRepository().readIndex(obj.getName()));
        }
    }

    private List<PrimitivePropertyKey> createTestPrimProps() throws GraphIntegrityException, InterruptedException,
            ExecutionException, InternalKeywordException, IOException, Exception {
        List<PrimitivePropertyKey> out = new ArrayList();
        PrimitivePropertyKey testProp1 = new PrimitivePropertyKey("testExportProp1", String.class, Cardinality.SINGLE);
        out.add(createAndReadPrimProp(testProp1));
        PrimitivePropertyKey testProp1Uniq = new PrimitivePropertyKey("testExportProp1Uniq", String.class, Cardinality.SINGLE);
        out.add(createAndReadPrimProp(testProp1Uniq));
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey<>("testExportProp2", Integer.class, Cardinality.SET, new HashSet<>(Arrays.asList(1, 2, 5)));
        out.add(createAndReadPrimProp(testProp2));
        PrimitivePropertyKey testProp3 = new PrimitivePropertyKey<>("testExportProp3", Geoshape.class, Cardinality.SET);
        out.add(createAndReadPrimProp(testProp3));
        PrimitivePropertyKey testProp4 = new PrimitivePropertyKey<>("testExportProp4", Character.class, Cardinality.SET);
        out.add(createAndReadPrimProp(testProp4));
        PrimitivePropertyKey testProp5 = new PrimitivePropertyKey<>("testExportProp5", Boolean.class, Cardinality.SET);
        out.add(createAndReadPrimProp(testProp5));
        PrimitivePropertyKey testProp6 = new PrimitivePropertyKey<>("testExportProp6", Byte.class, Cardinality.SET);
        out.add(createAndReadPrimProp(testProp6));
        PrimitivePropertyKey testProp7 = new PrimitivePropertyKey<>("testExportProp7", Short.class, Cardinality.SET);
        out.add(createAndReadPrimProp(testProp7));
        PrimitivePropertyKey testProp8 = new PrimitivePropertyKey<>("testExportProp8", Long.class, Cardinality.SET);
        out.add(createAndReadPrimProp(testProp8));
        PrimitivePropertyKey testProp9 = new PrimitivePropertyKey<>("testExportProp9", Float.class, Cardinality.SET);
        out.add(createAndReadPrimProp(testProp9));
        PrimitivePropertyKey testProp10 = new PrimitivePropertyKey<>("testExportProp10", Double.class, Cardinality.SET);
        out.add(createAndReadPrimProp(testProp10));
        PrimitivePropertyKey testProp11 = new PrimitivePropertyKey<>("testExportProp11", Date.class, Cardinality.SET);
        out.add(createAndReadPrimProp(testProp11));
        PrimitivePropertyKey testProp12 = new PrimitivePropertyKey<>("testExportProp12", UUID.class, Cardinality.SET);
        out.add(createAndReadPrimProp(testProp12));
        return out;
    }

    private List<ModelSchema> createTestModelSchema() throws GraphIntegrityException, InterruptedException,
            ExecutionException, InternalKeywordException, IOException, Exception {
        List<ModelSchema> out = new ArrayList();
        ModelSchemaRepository modelSchemaRepo = openFeatureStore.getModelSchemaRepository();
        ModelSchema newSchema = new ModelSchema(new HashSet<>(Arrays.asList("testExportProp1")));
        out.add(SchemaTest.createAndReadSchema(newSchema, modelSchemaRepo));
        return out;
    }

    private List<Model> createTestModel(String modelSchemaId) throws GraphIntegrityException, InterruptedException,
            ExecutionException, InternalKeywordException, IOException, Exception {
        List<Model> out = new ArrayList();
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testExportProp2", 1));
        fproperties.add(new Property("testExportProp3", Geoshape.geoshape(Geoshape.getShapeFactory().multiPoint()
                .pointXY(60.0, 60.0).pointXY(120.0, 60.0).build())));
        fproperties.add(new Property("testExportProp3", Geoshape.geoshape(Geoshape.getShapeFactory().multiLineString()
                .add(Geoshape.getShapeFactory().lineString().pointXY(59.0, 60.0).pointXY(61.0, 60.0))
                .add(Geoshape.getShapeFactory().lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0)).build())));
        fproperties.add(new Property("testExportProp4", 'e'));
        fproperties.add(new Property("testExportProp5", false));
        fproperties.add(new Property("testExportProp6", Byte.parseByte("6")));
        fproperties.add(new Property("testExportProp7", Short.parseShort("10")));
        fproperties.add(new Property("testExportProp8", 301L));
        fproperties.add(new Property("testExportProp9", 303.01f));
        fproperties.add(new Property("testExportProp10", 305.07d));
        fproperties.add(new Property("testExportProp11", new Date()));
        fproperties.add(new Property("testExportProp12", UUID.randomUUID()));
        fproperties.add(new Property("testExportProp1Uniq"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        List<Property> modelProps = new ArrayList<>();
        modelProps.add(new Property("testExportProp1", "test"));
        Model newModel = new Model(new HashSet<>(), modelProps, modelSchemaId, newFeatureSchema);
        out.add(createAndReadModel(newModel));
        return out;
    }

    private List<Feature> createTestFeatures(String modelId) throws GraphIntegrityException, InterruptedException,
            ExecutionException, InternalKeywordException, IOException, Exception {
        List<Feature> out = new ArrayList();
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testExportProp2", 5));
        featureProps.add(new Property("testExportProp1Uniq", "TestUNIQUEE"));
        Feature inpFeature = new Feature(modelId, featureProps);
        out.add(addAndReadFeature(inpFeature));
        return out;
    }

    private List<ViewSchema> createTestViewSchemas() throws GraphIntegrityException, InterruptedException,
            ExecutionException, InternalKeywordException, IOException, Exception {
        List<ViewSchema> out = new ArrayList();
        ViewSchema newSchema = new ViewSchema(Arrays.asList(new Property("testExportProp2", 5)));
        out.add(SchemaTest.createAndReadSchema(newSchema, openFeatureStore.getViewSchemaRepository()));
        return out;
    }

    private List<View> createTestViews(String viewSchemaId, String modelId) throws GraphIntegrityException, InterruptedException,
            ExecutionException, InternalKeywordException, IOException, Exception {
        List<View> out = new ArrayList();
        View view1 = new View(new HashSet<>(), new HashSet<>(), new ArrayList<>(), viewSchemaId);
        View outView = createAndReadView(view1);
        out.add(outView);
        View view = new View(new HashSet<>(Arrays.asList(modelId)),
                new HashSet<>(Arrays.asList(outView.getId())), new ArrayList<>(), viewSchemaId);
        out.add(createAndReadView(view));
        return out;
    }

    private List<RefPropertyKey> createTestRefProps(String modelId) throws GraphIntegrityException, InterruptedException,
            ExecutionException, InternalKeywordException, IOException, Exception {
        List<RefPropertyKey> out = new ArrayList();
        RefPropertyKey testProp = new RefPropertyKey("testpropref", modelId, Cardinality.SINGLE);
        out.add(createAndReadRefProp(testProp));
        return out;
    }

    private List<Index> createTestIndices() throws GraphIntegrityException, InterruptedException,
            ExecutionException, InternalKeywordException, IOException {
        List<Index> indices = new ArrayList();
        // create index 1
        Index index1 = new Index("testIndex", buildMixedIndexMap("testExportProp1"), IndexElementType.VERTEX, IndexType.MIXED, false, null);
        indices.add(openFeatureStore.getIndexRepository().createIndex(index1, false));
        // create index 2
        Index index = new Index("testIndex2", buildIndexMap("testExportProp2"), IndexElementType.EDGE, IndexType.COMPOSITE, false, null);
        indices.add(openFeatureStore.getIndexRepository().createIndex(index, false));
        // create index 3
        Index index3 = new Index("testIndex4Unique", buildIndexMap("testExportProp1Uniq"), IndexElementType.VERTEX, IndexType.COMPOSITE, true, null);
        indices.add(openFeatureStore.getIndexRepository().createIndex(index3, false));
        return indices;
    }

    private Path getExportDir() {
        return Paths.get("target/export").toAbsolutePath();
    }

    private PrimitivePropertyKey createAndReadPrimProp(PrimitivePropertyKey prop) throws Exception {
        openFeatureStore.getPropertyRepository().createProperty(prop);
        return openFeatureStore.getPropertyRepository().readPrimitiveProperty(prop.getName()).get();
    }

    private RefPropertyKey createAndReadRefProp(RefPropertyKey prop) throws Exception {
        openFeatureStore.getPropertyRepository().createProperty(prop);
        return openFeatureStore.getPropertyRepository().readRefProperty(prop.getName()).get();
    }

    private View createAndReadView(View input) throws Exception {
        String id = openFeatureStore.getViewRepository().createView(input);
        return openFeatureStore.getViewRepository().readView(id);
    }

    private Model createAndReadModel(Model inpModel) throws Exception {
        String modelId = openFeatureStore.getModelRepository().createModel(inpModel);
        return openFeatureStore.getModelRepository().readModel(modelId);
    }

    private Feature addAndReadFeature(Feature inpFeature) throws Exception {
        String id = openFeatureStore.getFeatureRepository().addFeature(inpFeature);
        return openFeatureStore.getFeatureRepository().readFeature(id);
    }

    private static Map<String, Parameter[]> buildMixedIndexMap(String indexKey) {
        Map<String, Parameter[]> paramMap = new HashMap<>();
        paramMap.put(indexKey, new Parameter[]{Mapping.TEXT.asParameter()});
        return paramMap;
    }

}
