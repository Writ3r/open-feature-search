/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.model;

import org.lwing.ofs.core.OpenFeatureStore;
import org.lwing.ofs.core.api.OFSVertex;
import org.lwing.ofs.core.api.config.OFSConfigurationParams;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.exception.VertexNotFoundException;
import org.lwing.ofs.core.api.feature.Feature;
import org.lwing.ofs.core.api.state.DependencyResource;
import org.lwing.ofs.core.api.lock.ResourceLock;
import org.lwing.ofs.core.api.model.Model;
import org.lwing.ofs.core.api.property.Cardinality;
import org.lwing.ofs.core.api.property.PrimitivePropertyKey;
import org.lwing.ofs.core.api.property.Property;
import org.lwing.ofs.core.api.property.RefPropertyKey;
import org.lwing.ofs.core.api.schema.Schema;
import org.lwing.ofs.core.impl.GraphTest;
import org.lwing.ofs.core.impl.property.PropertyRepository;
import org.lwing.ofs.core.impl.schema.SchemaTest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.lwing.ofs.core.api.schema.FeatureSchema;
import org.lwing.ofs.core.api.schema.ModelSchema;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;
import org.mockito.ArgumentCaptor;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 *
 * @author Lucas Wing
 */
public class ModelTest extends GraphTest {

    @Test
    public void testAddModel() throws Exception {
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testprop2", Long.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp3 = new PrimitivePropertyKey("testprop3", String.class, Cardinality.SINGLE);
        for (PrimitivePropertyKey key : Arrays.asList(testProp, testProp2, testProp3)) {
            repo.createProperty(key);
        }
        // create model schema
        Schema outModelSchema = makeBasicModelSchema(openFeatureStore);
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        Property prop1 = new Property("testprop", "test");
        Property prop2 = new Property("testprop2", 2L);
        fproperties.add(prop1);
        fproperties.add(prop2);
        fproperties.add(new Property("testprop3"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        List<Property> modelProps = new ArrayList<>();
        modelProps.add(new Property(BASIC_MODEL_PROP_NAME, "test"));
        Model inpModel = new Model(new HashSet<>(), modelProps, outModelSchema.getId(), newFeatureSchema);
        Model newModelRet = createAndReadModel(inpModel);
        // verify
        inpModel.getFeatureSchema().getDefaultProperties().clear();
        inpModel.getFeatureSchema().getDefaultProperties().add(prop1);
        inpModel.getFeatureSchema().getDefaultProperties().add(prop2);
        verifyModelEquals(inpModel, newModelRet);
    }

    @Test
    public void testAddModelCustId() throws Exception {
        // create schema
        ModelSchema outSchema = createAndReadModelSchema(new ModelSchema(new HashSet<>()));
        // create model
        Model inpModel = new Model(
                "testId",
                new HashSet<>(),
                new ArrayList<>(),
                outSchema.getId(),
                new FeatureSchema(new ArrayList<>())
        );
        Model newModelRet = createAndReadModel(inpModel);
        // verify
        assertEquals("testId", newModelRet.getId());
    }

    @Test
    public void testAddModelPropsNotInSchema() throws Exception {
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testprop2", Long.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp3 = new PrimitivePropertyKey("testprop3", String.class, Cardinality.SINGLE);
        for (PrimitivePropertyKey key : Arrays.asList(testProp, testProp2, testProp3)) {
            repo.createProperty(key);
        }
        // create model schema
        Schema outModelSchema = makeBasicModelSchema(openFeatureStore);
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop", "test"));
        fproperties.add(new Property("testprop2", 2L));
        fproperties.add(new Property("testprop3"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        List<Property> modelProps = new ArrayList<>();
        modelProps.add(new Property(BASIC_MODEL_PROP_NAME, "test"));
        modelProps.add(new Property("testprop", "test"));
        modelProps.add(new Property("testprop2", 2L));
        Model inpModel = new Model(new HashSet<>(), modelProps, outModelSchema.getId(), newFeatureSchema);
        Model newModelRet = createAndReadModel(inpModel);
        // verify
        assertEquals(1, newModelRet.getProperties().size());
        assertEquals(BASIC_MODEL_PROP_NAME, newModelRet.getProperties().get(0).getName());
    }

    @Test
    public void testAddModelNotAllSchemaProps() throws Exception {
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testprop2", Long.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp3 = new PrimitivePropertyKey("testprop3", String.class, Cardinality.SINGLE);
        for (PrimitivePropertyKey key : Arrays.asList(testProp, testProp2, testProp3)) {
            repo.createProperty(key);
        }
        // create model schema
        Schema outModelSchema = makeBasicModelSchema(openFeatureStore);
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop", "test"));
        fproperties.add(new Property("testprop2", 2L));
        fproperties.add(new Property("testprop3"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        List<Property> modelProps = new ArrayList<>();
        modelProps.add(new Property("propNotInScheam", "test"));
        Model inpModel = new Model(new HashSet<>(), modelProps, outModelSchema.getId(), newFeatureSchema);
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getModelRepository().createModel(inpModel);
        });
    }

    @Test
    public void testAddModelAllowableFields() throws Exception {
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey<>("testprop", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey<>("testprop3",
                String.class, Cardinality.SET, new HashSet<>(Arrays.asList("testAllow", "testAllow2")));
        for (PrimitivePropertyKey key : Arrays.asList(testProp, testProp2)) {
            repo.createProperty(key);
        }
        // create model schema
        ModelSchema newSchema = new ModelSchema(new HashSet<>(Arrays.asList("testprop", "testprop3")));
        ModelSchema modelSchema = createAndReadModelSchema(newSchema);
        // create model
        List<Property> modelProps = new ArrayList<>();
        modelProps.add(new Property("testprop", "test"));
        modelProps.add(new Property("testprop3", "testAllow"));
        Model inpModel = new Model(new HashSet<>(), modelProps, modelSchema.getId(), new FeatureSchema(Arrays.asList()));
        Model newModelRet = createAndReadModel(inpModel);
        // verify good
        verifyModelEquals(inpModel, newModelRet);
        // attempt create model with invalid allow value
        modelProps = new ArrayList<>();
        modelProps.add(new Property("testprop", "test"));
        modelProps.add(new Property("testprop3", "testAllowNO"));
        modelProps.add(new Property("testprop3", "testAllow"));
        Model inpModel2 = new Model(new HashSet<>(), modelProps, modelSchema.getId(), new FeatureSchema(Arrays.asList()));
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getModelRepository().createModel(inpModel2);
        });
    }

    @Test
    public void testAddModelDefaultSchemaProps() throws Exception {
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        repo.createProperty(testProp);
        // create model schema
        PrimitivePropertyKey testModelProp = new PrimitivePropertyKey(BASIC_MODEL_PROP_NAME, String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testModelProp2 = new PrimitivePropertyKey("modelProp2", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testModelProp3 = new PrimitivePropertyKey("modelProp3", String.class, Cardinality.SET);
        repo.createProperty(testModelProp);
        repo.createProperty(testModelProp2);
        repo.createProperty(testModelProp3);
        List<Property> modelProperties = new ArrayList<>();
        modelProperties.add(new Property("testModelprop"));
        Property primitiveProp1 = new Property("modelProp2", "abc");
        modelProperties.add(primitiveProp1);
        Property primitiveProp2 = new Property("modelProp3", "aaaa");
        modelProperties.add(primitiveProp2);
        Property primitiveProp3 = new Property("modelProp3", "bbbb");
        modelProperties.add(primitiveProp3);
        ModelSchema newSchema = new ModelSchema(modelProperties);
        Schema outModelSchema = createAndReadModelSchema(newSchema);
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop", "test"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        List<Property> modelProps = new ArrayList<>();
        modelProps.add(new Property(BASIC_MODEL_PROP_NAME, "test"));
        Model inpModel = new Model(new HashSet<>(), modelProps, outModelSchema.getId(), newFeatureSchema);
        Model newModelRet = createAndReadModel(inpModel);
        // verify
        List<Property> expectedProps = new ArrayList();
        for (Property prop : Arrays.asList(primitiveProp1, primitiveProp2, primitiveProp3)) {
            expectedProps.add(new Property(prop.getName(), prop.getValue()));
        }
        expectedProps.addAll(inpModel.getProperties());
        verifyModelEquals(inpModel, newModelRet, expectedProps);
    }

    @Test
    public void testReadModel() throws Exception {
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testprop2", Long.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp3 = new PrimitivePropertyKey("testprop3", String.class, Cardinality.SINGLE);
        for (PrimitivePropertyKey key : Arrays.asList(testProp, testProp2, testProp3)) {
            repo.createProperty(key);
        }
        // create model schema
        Schema outModelSchema = makeBasicModelSchema(openFeatureStore);
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        Property prop1 = new Property("testprop", "test");
        Property prop2 = new Property("testprop2", 2L);
        fproperties.add(prop1);
        fproperties.add(prop2);
        fproperties.add(new Property("testprop3"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        List<Property> modelProps = new ArrayList<>();
        modelProps.add(new Property(BASIC_MODEL_PROP_NAME, "test"));
        Model inpModel = new Model(new HashSet<>(), modelProps, outModelSchema.getId(), newFeatureSchema);
        Model newModelRet = createAndReadModel(inpModel);
        // verify
        Model readModel = openFeatureStore.getModelRepository().readModel(newModelRet.getId());
        inpModel.getFeatureSchema().getDefaultProperties().clear();
        inpModel.getFeatureSchema().getDefaultProperties().add(prop1);
        inpModel.getFeatureSchema().getDefaultProperties().add(prop2);
        verifyModelEquals(inpModel, readModel);
    }

    @Test
    public void testReadModelInvalidObject() throws Exception {
        // setup
        Schema outModelSchema = makeBasicModelSchema(openFeatureStore);
        // read with select
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getModelRepository().readModel(outModelSchema.getId());
        });
    }

    @Test
    public void testModelExtends() throws Exception {
        ModelRepository modelRepo = openFeatureStore.getModelRepository();
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testprop2", Long.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp3 = new PrimitivePropertyKey("testprop3", String.class, Cardinality.SINGLE);
        for (PrimitivePropertyKey key : Arrays.asList(testProp, testProp2, testProp3)) {
            repo.createProperty(key);
        }
        // create model schema
        Schema outModelSchema = makeBasicModelSchema(openFeatureStore);
        // build model props
        List<Property> modelProps = new ArrayList<>();
        modelProps.add(new Property(BASIC_MODEL_PROP_NAME, "test"));
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop", "test"));
        fproperties.add(new Property("testprop2", 2L));
        fproperties.add(new Property("testprop3"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // build model parent 1
        List<Property> fproperties2 = new ArrayList<>();
        fproperties2.add(new Property("testprop", "test"));
        FeatureSchema newFeatureSchema2 = new FeatureSchema(fproperties2);
        Model inpModel = new Model(new HashSet<>(), modelProps, outModelSchema.getId(), newFeatureSchema2);
        Model newModelRet1 = createAndReadModel(inpModel);
        // build model parent 2
        List<Property> fproperties3 = new ArrayList<>();
        fproperties3.add(new Property("testprop2", 2L));
        FeatureSchema newFeatureSchema3 = new FeatureSchema(fproperties3);
        Model inpModel2 = new Model(new HashSet<>(), modelProps, outModelSchema.getId(), newFeatureSchema3);
        Model newModelRet2 = createAndReadModel(inpModel2);
        // build model
        Model inpModel3 = new Model(new HashSet<>(Arrays.asList(newModelRet1.getId(), newModelRet2.getId())),
                modelProps, outModelSchema.getId(), newFeatureSchema);
        Model newModelRet3 = createAndReadModel(inpModel3);
        assertEquals(inpModel3.getInheritsFromIds(), newModelRet3.getInheritsFromIds());
    }

    @Test
    public void testModelDelete() throws Exception {
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        for (PrimitivePropertyKey key : Arrays.asList(testProp)) {
            repo.createProperty(key);
        }
        // create model schema
        Schema outModelSchema = makeBasicModelSchema(openFeatureStore);
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop", "test"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        List<Property> modelProps = new ArrayList<>();
        modelProps.add(new Property(BASIC_MODEL_PROP_NAME, "test"));
        Model inpModel = new Model(new HashSet<>(), modelProps, outModelSchema.getId(), newFeatureSchema);
        Model newModelRet = createAndReadModel(inpModel);
        // delete
        openFeatureStore.getModelRepository().deleteModel(newModelRet.getId());
        assertThrows(VertexNotFoundException.class, () -> {
            openFeatureStore.getModelRepository().readModel(newModelRet.getId());
        });
    }

    @Test
    public void testModelDeleteButHasReference() throws Exception {;
        ModelRepository modelRepo = openFeatureStore.getModelRepository();
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        for (PrimitivePropertyKey key : Arrays.asList(testProp)) {
            repo.createProperty(key);
        }
        // create model schema
        Schema outModelSchema = makeBasicModelSchema(openFeatureStore);
        // build model props
        List<Property> modelProps = new ArrayList<>();
        modelProps.add(new Property(BASIC_MODEL_PROP_NAME, "test"));
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop", "test"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // build model parent 1
        List<Property> fproperties2 = new ArrayList<>();
        fproperties2.add(new Property("testprop", "test"));
        FeatureSchema newFeatureSchema2 = new FeatureSchema(fproperties2);
        Model inpModel = new Model(new HashSet<>(), modelProps, outModelSchema.getId(), newFeatureSchema2);
        Model newModelRet1 = createAndReadModel(inpModel);
        // build model
        Model inpModel3 = new Model(new HashSet<>(Arrays.asList(newModelRet1.getId())),
                modelProps, outModelSchema.getId(), newFeatureSchema);
        Model newModelRet3 = createAndReadModel(inpModel3);
        // delete parent
        assertThrows(GraphIntegrityException.class, () -> {
            modelRepo.deleteModel(newModelRet1.getId());
        });
    }

    @Test
    public void testSearchIterator() throws Exception {
        ModelRepository modelRepo = openFeatureStore.getModelRepository();
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        repo.createProperty(testProp);
        // create model schema
        Schema outModelSchema = makeBasicModelSchema(openFeatureStore);
        // build model props
        List<Property> modelProps = new ArrayList<>();
        modelProps.add(new Property(BASIC_MODEL_PROP_NAME, "test"));
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop", "test"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // build model parent 1
        List<Property> fproperties2 = new ArrayList<>();
        fproperties2.add(new Property("testprop", "test"));
        FeatureSchema newFeatureSchema2 = new FeatureSchema(fproperties2);
        Model inpModel = new Model(new HashSet<>(), modelProps, outModelSchema.getId(), newFeatureSchema2);
        Model newModelRet1 = createAndReadModel(inpModel);
        // build model
        Model inpModel3 = new Model(new HashSet<>(Arrays.asList(newModelRet1.getId())),
                modelProps, outModelSchema.getId(), newFeatureSchema);
        Model newModelRet3 = createAndReadModel(inpModel3);
        // TODO: add a feature & view here to ensure those aren't picked up
        // exec search
        List<Model> outList = new ArrayList();
        openFeatureStore.getModelRepository().search((g) -> {
            return g.V();
        }, (featureIterator) -> {
            outList.add(featureIterator.next());
            outList.add(featureIterator.next());
            assertTrue(!featureIterator.hasNext());
        });
        // check models exist
        assertTrue(getVertexFromList(outList, newModelRet1.getId()) != null);
        assertTrue(getVertexFromList(outList, newModelRet3.getId()) != null);
    }

    @Test
    public void testSearchList() throws Exception {
        ModelRepository modelRepo = openFeatureStore.getModelRepository();
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        repo.createProperty(testProp);
        // create model schema
        Schema outModelSchema = makeBasicModelSchema(openFeatureStore);
        // build model props
        List<Property> modelProps = new ArrayList<>();
        modelProps.add(new Property(BASIC_MODEL_PROP_NAME, "test"));
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop", "test"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // build model parent 1
        List<Property> fproperties2 = new ArrayList<>();
        fproperties2.add(new Property("testprop", "test"));
        FeatureSchema newFeatureSchema2 = new FeatureSchema(fproperties2);
        Model inpModel = new Model(new HashSet<>(), modelProps, outModelSchema.getId(), newFeatureSchema2);
        Model newModelRet1 = createAndReadModel(inpModel);
        // build model
        Model inpModel3 = new Model(new HashSet<>(Arrays.asList(newModelRet1.getId())),
                modelProps, outModelSchema.getId(), newFeatureSchema);
        Model newModelRet3 = createAndReadModel(inpModel3);
        // TODO: add a feature & view here to ensure those aren't picked up
        // exec search
        List<Model> outList = openFeatureStore.getModelRepository().search((g) -> {
            return g.V();
        });
        // check models exist
        assertEquals(2, outList.size());
        assertTrue(getVertexFromList(outList, newModelRet1.getId()) != null);
        assertTrue(getVertexFromList(outList, newModelRet3.getId()) != null);
    }

    @Test
    public void testReadModelWithSelect() throws Exception {
        // SETUP ADDING MODELS & FEATURES TO REF
        // =====================================
        Model newModelRet1 = createBasicModel(openFeatureStore);
        Feature testOut1 = createBasicFeature(openFeatureStore, newModelRet1);
        // setup test
        Model newModel = setupSelectTest(newModelRet1, testOut1);
        Set<String> selectSet = new HashSet<>(Arrays.asList("testprimprop2", "testrefprop2"));
        Model outModel = openFeatureStore.getModelRepository().readModel(newModel.getId(), selectSet);
        // verify outputs
        verifyModelSelect(testOut1, newModel, outModel);
    }

    @Test
    public void testSearchModelWithSelect() throws Exception {
        // SETUP ADDING MODELS & FEATURES TO REF
        // =====================================
        Model newModelRet1 = createBasicModel(openFeatureStore);
        Feature testOut1 = createBasicFeature(openFeatureStore, newModelRet1);
        // setup test
        Model newModel = setupSelectTest(newModelRet1, testOut1);
        Set<String> selectSet = new HashSet<>(Arrays.asList("testprimprop2", "testrefprop2"));
        Model outModel = openFeatureStore.getModelRepository().search(
                (g) -> {
                    return g.V(newModel.getId());
                }, Optional.of(selectSet)).get(0);
        // verify outputs
        verifyModelSelect(testOut1, newModel, outModel);
    }

    private void verifyModelSelect(Feature testOut1, Model newModelCreated, Model outModel) {
        List<Property> featurePropsCheck = new ArrayList<>();
        newModelCreated.getProperties().clear();
        newModelCreated.getProperties().add(new Property("testrefprop2", testOut1.getId()));
        newModelCreated.getProperties().add(new Property("testprimprop2", "aaa"));
        newModelCreated.getProperties().addAll(featurePropsCheck);
        verifyModelEquals(newModelCreated, outModel);
    }

    private Model setupSelectTest(Model inpModel, Feature newFeature) throws Exception {
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprimprop", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testprimprop2", String.class, Cardinality.LIST);
        RefPropertyKey testRefProp = new RefPropertyKey("testrefprop", inpModel.getId(), Cardinality.SINGLE);
        RefPropertyKey testRefProp2 = new RefPropertyKey("testrefprop2", inpModel.getId(), Cardinality.LIST);
        repo.createProperty(testProp);
        repo.createProperty(testProp2);
        repo.createProperty(testRefProp);
        repo.createProperty(testRefProp2);
        // create model schema
        List<Property> sproperties = new ArrayList<>();
        sproperties.add(new Property("testprimprop"));
        sproperties.add(new Property("testprimprop2"));
        sproperties.add(new Property("testrefprop"));
        sproperties.add(new Property("testrefprop2"));
        ModelSchema newModelSchema = new ModelSchema(sproperties);
        Schema outModelSchema = createAndReadModelSchema(newModelSchema);
        // create model
        List<Property> modelProps = new ArrayList<>();
        modelProps.add(new Property("testrefprop", newFeature.getId()));
        modelProps.add(new Property("testrefprop2", newFeature.getId()));
        modelProps.add(new Property("testprimprop", "testProp"));
        modelProps.add(new Property("testprimprop2", "aaa"));
        Model outModel = new Model(new HashSet<>(), modelProps, outModelSchema.getId(), new FeatureSchema(Arrays.asList()));
        return createAndReadModel(outModel);
    }

    @Test
    public void testModificationDuringSearch() throws Exception {
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        repo.createProperty(testProp);
        // create model schema
        Schema outModelSchema = makeBasicModelSchema(openFeatureStore);
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop", "test"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        List<Property> modelProps = new ArrayList<>();
        modelProps.add(new Property(BASIC_MODEL_PROP_NAME, "test"));
        Model inpModel = new Model(new HashSet<>(), modelProps, outModelSchema.getId(), newFeatureSchema);
        Model newModelRet = createAndReadModel(inpModel);
        // exec search
        assertThrows(VerificationException.class, () -> {
            openFeatureStore.getModelRepository().search((g) -> {
                try ( Transaction tx = g.tx()) {
                    g.V(newModelRet.getId()).property("testprop", "test").next();
                    tx.commit();
                }
                return g.V(newModelRet.getId());
            });
        });
    }

    @Test
    public void testModelDefaultFeatureProperties() throws Exception {
        // create default model & feature
        Model newModelRet = createBasicModel(openFeatureStore);
        Feature newFeature = createBasicFeature(openFeatureStore, newModelRet);
        Feature newFeature2 = createBasicFeature(openFeatureStore, newModelRet);
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testModelpropNormal", String.class, Cardinality.SINGLE);
        RefPropertyKey testProp2 = new RefPropertyKey("testModelpropRef", newModelRet.getId(), Cardinality.SINGLE);
        RefPropertyKey testProp3 = new RefPropertyKey("testModelpropRef3", newModelRet.getId(), Cardinality.SINGLE);
        repo.createProperty(testProp);
        repo.createProperty(testProp2);
        repo.createProperty(testProp3);
        // create model schema
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testModelpropNormal"));
        properties.add(new Property("testModelpropRef", newFeature.getId()));
        properties.add(new Property("testModelpropRef3", newFeature2.getId()));
        ModelSchema newModelSchema = new ModelSchema(properties);
        ModelSchema newModelSchemaOut = createAndReadModelSchema(newModelSchema);
        // build model props
        List<Property> modelProps = new ArrayList<>();
        modelProps.add(new Property("testModelpropNormal", "test"));
        // build model
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop", "test"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        Model inpModel3 = new Model(new HashSet<>(), modelProps, newModelSchemaOut.getId(), newFeatureSchema);
        Model newModelRet3 = createAndReadModel(inpModel3);
        // verify
        inpModel3.getProperties().add(new Property("testModelpropRef", newFeature.getId()));
        inpModel3.getProperties().add(new Property("testModelpropRef3", newFeature2.getId()));
        verifyModelEquals(inpModel3, newModelRet3);
    }

    @Test
    public void testModelFeatureProperties() throws Exception {
        // create default model & feature
        Model newModelRet = createBasicModel(openFeatureStore);
        Feature newFeature = createBasicFeature(openFeatureStore, newModelRet);
        Feature newFeature2 = createBasicFeature(openFeatureStore, newModelRet);
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testModelpropNormal", String.class, Cardinality.SINGLE);
        RefPropertyKey testProp2 = new RefPropertyKey("testModelpropRef", newModelRet.getId(), Cardinality.SINGLE);
        RefPropertyKey testProp3 = new RefPropertyKey("testModelpropRef3", newModelRet.getId(), Cardinality.SINGLE);
        repo.createProperty(testProp);
        repo.createProperty(testProp2);
        repo.createProperty(testProp3);
        // create model schema
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testModelpropNormal"));
        properties.add(new Property("testModelpropRef"));
        properties.add(new Property("testModelpropRef3"));
        ModelSchema newModelSchema = new ModelSchema(properties);
        ModelSchema newModelSchemaOut = createAndReadModelSchema(newModelSchema);
        // build model props
        List<Property> modelProps = new ArrayList<>();
        modelProps.add(new Property("testModelpropNormal", "test"));
        modelProps.add(new Property("testModelpropRef", newFeature.getId()));
        modelProps.add(new Property("testModelpropRef3", newFeature2.getId()));
        // build model
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop", "test"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        Model inpModel3 = new Model(new HashSet<>(), modelProps, newModelSchemaOut.getId(), newFeatureSchema);
        Model newModelRet3 = createAndReadModel(inpModel3);
        // verify
        verifyModelEquals(inpModel3, newModelRet3);
    }

    @Test
    public void testFeatureSchemaDeletedWithModel() throws Exception {
        // create model
        Model newModelRet = createBasicModel(openFeatureStore);
        String featureSchemaId = newModelRet.getFeatureSchema().getId();
        // delete model
        openFeatureStore.getModelRepository().deleteModel(newModelRet.getId());
        // verify
        assertThrows(VertexNotFoundException.class, () -> {
            openFeatureStore.getFeatureSchemaRepository().readSchema(featureSchemaId);
        });
    }

    @Test
    public void testModelRefPropDoesntFitModelSchema() throws Exception {
        // create default model & feature
        Model newModelRet = createBasicModel(openFeatureStore);
        Model newModelRet2 = createBasicModel(openFeatureStore, false);
        Feature newFeature = createBasicFeature(openFeatureStore, newModelRet);
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testProp3 = new RefPropertyKey("testModelpropRef3", newModelRet2.getId(), Cardinality.SINGLE);
        repo.createProperty(testProp3);
        // create model schema
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testModelpropRef3"));
        ModelSchema newModelSchema = new ModelSchema(properties);
        ModelSchema newModelSchemaOut = createAndReadModelSchema(newModelSchema);
        // build model props
        List<Property> modelProps = new ArrayList<>();
        modelProps.add(new Property("testModelpropRef3", newFeature.getId()));
        // build model
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop", "test"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        Model inpModel3 = new Model(new HashSet<>(), modelProps, newModelSchemaOut.getId(), newFeatureSchema);
        // verify add fails because we tried to add a prop which doesn't match the model which the schema points at
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getModelRepository().createModel(inpModel3);
        });
    }

    @Test
    public void testModelLockDuringCreation() throws Exception {
        // build model parent 1
        Schema modelSchema = createAndReadModelSchema(new ModelSchema(Arrays.asList()));
        Model inpModel = new Model(new HashSet<>(), new ArrayList<>(), modelSchema.getId(), new FeatureSchema(Arrays.asList()));
        Model newModelRet1 = createAndReadModel(inpModel);
        // build model partent 2
        Model newModelRet2 = createAndReadModel(inpModel);
        // create custom ofs with mocks for locks
        ArgumentCaptor<Set<String>> lockAcquireCaptor = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<Set<String>> lockReleaseCaptor = ArgumentCaptor.forClass(Set.class);
        ResourceLock mockLock = mock(ResourceLock.class);
        OpenFeatureStore customOFS = getOpenFeatureStore(OFSConfigurationParams.build().setResourceLock(mockLock));
        // exec
        Model inpModel3 = new Model(new HashSet<>(Arrays.asList(newModelRet1.getId(), newModelRet2.getId())),
                Arrays.asList(), modelSchema.getId(), new FeatureSchema(Arrays.asList()));
        customOFS.getModelRepository().createModel(inpModel3);
        // verify
        verify(mockLock, times(2)).acquireLocks(lockAcquireCaptor.capture());
        verify(mockLock, times(2)).releaseLocks(lockReleaseCaptor.capture());
        Set<String> expectedLockResources = new HashSet<>();
        expectedLockResources.add(DependencyResource.fromNodeId(OFSType.MODEL, newModelRet2.getId()).getResource());
        expectedLockResources.add(DependencyResource.fromNodeId(OFSType.MODEL, newModelRet1.getId()).getResource());
        expectedLockResources.add(DependencyResource.fromNodeId(OFSType.MODEL_SCHEMA, modelSchema.getId()).getResource());
        // the second value is the feature schema lock which I don't care about here, so just chceck the first value
        assertEquals(expectedLockResources, lockAcquireCaptor.getAllValues().get(0));
        assertEquals(expectedLockResources, lockReleaseCaptor.getAllValues().get(1));
    }

    @Test
    public void testModelLockDuringDeletion() throws Exception {
        Model newModelRet = createBasicModel(openFeatureStore);
        // create custom ofs with mocks for locks
        ArgumentCaptor<Set<String>> lockAcquireCaptor = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<Set<String>> lockReleaseCaptor = ArgumentCaptor.forClass(Set.class);
        ResourceLock mockLock = mock(ResourceLock.class);
        OpenFeatureStore customOFS = getOpenFeatureStore(OFSConfigurationParams.build().setResourceLock(mockLock));
        // exec
        customOFS.getModelRepository().deleteModel(newModelRet.getId());
        // verify
        verify(mockLock, times(2)).acquireLocks(lockAcquireCaptor.capture());
        verify(mockLock, times(2)).releaseLocks(lockReleaseCaptor.capture());
        Set<String> expectedLockResources = new HashSet<>();
        expectedLockResources.add(DependencyResource.fromNodeId(OFSType.MODEL, newModelRet.getId()).getResource());
        // the second value is the feature schema lock which I don't care about here, so just chceck the first value
        assertEquals(expectedLockResources, lockAcquireCaptor.getAllValues().get(0));
        assertEquals(expectedLockResources, lockReleaseCaptor.getAllValues().get(1));
    }

    public static void verifyModelEquals(Model expectedModel, Model givenModel) {
        verifyModelEquals(expectedModel, givenModel, expectedModel.getProperties());
    }

    private static void verifyModelEquals(Model expectedModel, Model givenModel, List<Property> expectedModelProperties) {
        // verify inherits
        HashSet<String> expectedInherits = new HashSet<>(expectedModel.getInheritsFromIds());
        assertEquals(expectedModel.getInheritsFromIds().size(), givenModel.getInheritsFromIds().size());
        expectedInherits.removeAll(givenModel.getInheritsFromIds());
        assertEquals(0, expectedInherits.size());
        // verify model schema
        assertEquals(expectedModel.getModelSchemaId(), givenModel.getModelSchemaId());
        // verify feature schema
        SchemaTest.verifySchemaEquals(expectedModel.getFeatureSchema(), givenModel.getFeatureSchema());
        // verify properties
        verifyPropertiesEqual(expectedModelProperties, givenModel.getProperties());
    }

    public static void verifyPropertiesEqual(List<Property> expectedProperties, List<Property> givenProperties) {
        assertEquals(expectedProperties.size(), givenProperties.size());
        Map<String, List<Object>> givenNameToValues = buildPropertyMap(givenProperties);
        Map<String, List<Object>> expectedNameToValues = buildPropertyMap(expectedProperties);
        for (String propKey : expectedNameToValues.keySet()) {
            assertTrue(expectedNameToValues.containsKey(propKey));
            List<Object> expectedDefaults = expectedNameToValues.get(propKey);
            List<Object> givenDefaults = givenNameToValues.get(propKey);
            assertTrue(expectedDefaults.size() == givenDefaults.size()
                    && expectedDefaults.containsAll(givenDefaults)
                    && givenDefaults.containsAll(expectedDefaults));
        }
    }

    private static Map<String, List<Object>> buildPropertyMap(List<Property> modelProperties) {
        Map<String, List<Object>> nameToValues = new HashMap<>();
        modelProperties.stream().forEach(p -> {
            List<Object> defaults = nameToValues.getOrDefault(p.getName(), new ArrayList());
            defaults.add(p.getValue());
            nameToValues.put(p.getName(), defaults);
        });
        return nameToValues;
    }

    private <E extends OFSVertex> E getVertexFromList(List<E> osvertexes, Object id) throws Exception {
        for (E vertex : osvertexes) {
            if (vertex.getId().equals(id)) {
                return vertex;
            }
        }
        throw new Exception("Failed to find vertex with id " + id);
    }
    
    private Model createAndReadModel(Model inpModel) throws Exception {
        String modelId = openFeatureStore.getModelRepository().createModel(inpModel);
        return openFeatureStore.getModelRepository().readModel(modelId);
    }
    
    private ModelSchema createAndReadModelSchema(ModelSchema schema) throws Exception {
        String schemaId = openFeatureStore.getModelSchemaRepository().createSchema(schema);
        return openFeatureStore.getModelSchemaRepository().readSchema(schemaId);
    }

}
