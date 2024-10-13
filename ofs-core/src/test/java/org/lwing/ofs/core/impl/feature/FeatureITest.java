/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.feature;

import org.lwing.ofs.core.OpenFeatureStore;
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
import org.lwing.ofs.core.impl.GraphTest;
import org.lwing.ofs.core.impl.model.ModelTest;
import org.lwing.ofs.core.impl.property.PropertyRepository;
import org.lwing.ofs.core.impl.model.ModelSchemaRepository;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import org.lwing.ofs.core.api.schema.FeatureSchema;
import org.lwing.ofs.core.api.schema.ModelSchema;
import org.lwing.ofs.core.api.schema.Schema;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;
import org.mockito.ArgumentCaptor;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 *
 * @author Lucas Wing
 */
public class FeatureITest extends GraphTest {

    @Test
    public void testAddFeature() throws Exception {
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testprop2", Long.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp3 = new PrimitivePropertyKey("testprop3", String.class, Cardinality.SINGLE);
        for (PrimitivePropertyKey key : Arrays.asList(testProp, testProp2, testProp3)) {
            repo.createProperty(key);
        }
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop", "test"));
        fproperties.add(new Property("testprop2", 2L));
        fproperties.add(new Property("testprop3"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        Model newModelRet = createBasicModel(openFeatureStore, newFeatureSchema);
        // create feature
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testprop", "test"));
        featureProps.add(new Property("testprop2", 2L));
        featureProps.add(new Property("testprop3", "test"));
        Feature inpFeature = new Feature(newModelRet.getId(), featureProps);
        Feature outFeature = addAndReadFeature(inpFeature);
        inpFeature.getInheritsFrom().add(newModelRet.getId());
        verifyFeatureEquals(inpFeature, outFeature);
    }

    @Test
    public void testAddFeatureWithId() throws Exception {
        Model newModelRet = createBasicModel(openFeatureStore);
        // create actual view
        Feature inpFeature = new Feature("testId", newModelRet.getId(), new ArrayList<>());
        Feature outFeature = addAndReadFeature(inpFeature);
        // verify id
        assertEquals("testId", outFeature.getId());
    }

    @Test
    public void testReadFeatureBasic() throws Exception {
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testprop2", Long.class, Cardinality.SINGLE);
        for (PrimitivePropertyKey key : Arrays.asList(testProp, testProp2)) {
            repo.createProperty(key);
        }
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop"));
        fproperties.add(new Property("testprop2"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        Model newModelRet = createBasicModel(openFeatureStore, newFeatureSchema);
        // create feature
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testprop", "test"));
        featureProps.add(new Property("testprop2", 2L));
        Feature inpFeature = new Feature(newModelRet.getId(), featureProps);
        Feature outFeature = addAndReadFeature(inpFeature);
        inpFeature.getInheritsFrom().add(newModelRet.getId());
        verifyFeatureEquals(inpFeature, openFeatureStore.getFeatureRepository().readFeature(outFeature.getId()));
    }

    @Test
    public void testFeatureInheritsFrom() throws Exception {
        // create supertype models
        Model newModelRet1 = createBasicModel(openFeatureStore);
        Model newModelRet2 = createBasicModel(openFeatureStore, false);
        Model newModelRet3 = createBasicModel(openFeatureStore, new FeatureSchema(new ArrayList<>()), new HashSet(Arrays.asList(newModelRet1.getId())), false);
        Model newModelRet4 = createBasicModel(openFeatureStore, new FeatureSchema(new ArrayList<>()), new HashSet(Arrays.asList(newModelRet3.getId(), newModelRet2.getId())), false);
        // create subtype model & feature from that model
        Model newModel = createBasicModel(openFeatureStore, new FeatureSchema(new ArrayList<>()), new HashSet(Arrays.asList(newModelRet4.getId())), false);
        Feature testOut1 = createBasicFeature(openFeatureStore, newModel);
        // verify feature
        Feature checkOut = new Feature(testOut1.getModelId(), testOut1.getProperties());
        checkOut.getInheritsFrom().addAll(Arrays.asList(newModel.getId(), newModelRet1.getId(), newModelRet2.getId(), newModelRet3.getId(), newModelRet4.getId()));
        verifyFeatureEquals(checkOut, testOut1);
    }

    @Test
    public void testModelInheritance() throws Exception {
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testpropa", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testpropa2", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp3 = new PrimitivePropertyKey("testpropa3", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp4 = new PrimitivePropertyKey("testpropa4", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp5 = new PrimitivePropertyKey("testpropa5", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp6 = new PrimitivePropertyKey("testpropa6", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp7 = new PrimitivePropertyKey("testpropa7", String.class, Cardinality.SINGLE);
        repo.createProperty(testProp);
        repo.createProperty(testProp2);
        repo.createProperty(testProp3);
        repo.createProperty(testProp4);
        repo.createProperty(testProp5);
        repo.createProperty(testProp6);
        repo.createProperty(testProp7);
        // create model 1 teir 3
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testpropa", "test"));
        fproperties.add(new Property("testpropa2", "test2"));
        fproperties.add(new Property("testpropa7", "test7"));
        fproperties.add(new Property("testpropa3"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        Model newModelRet1 = createBasicModel(openFeatureStore, newFeatureSchema);
        // create model 2 teir 3
        List<Property> fproperties2 = new ArrayList<>();
        fproperties2.add(new Property("testpropa4"));
        fproperties2.add(new Property("testpropa5", "test3"));
        FeatureSchema newFeatureSchema2 = new FeatureSchema(fproperties2);
        Model newModelRet2 = createBasicModel(openFeatureStore, newFeatureSchema2, false);
        // create model 1 tier 2
        List<Property> fproperties3 = new ArrayList<>();
        fproperties3.add(new Property("testpropa2", "testOverride1"));
        fproperties3.add(new Property("testpropa4", "testValue2"));
        fproperties3.add(new Property("testpropa6"));
        FeatureSchema newFeatureSchema3 = new FeatureSchema(fproperties3);
        Model newModelRet3 = createBasicModel(openFeatureStore,
                newFeatureSchema3, new HashSet<>(Arrays.asList(newModelRet1.getId(), newModelRet2.getId())), false);
        // create feature
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testpropa3", "testVal3"));
        featureProps.add(new Property("testpropa7", "testOverride3"));
        featureProps.add(new Property("testpropa6", "testVal6"));
        Feature inpFeature = new Feature(newModelRet3.getId(), featureProps);
        Feature outFeature = addAndReadFeature(inpFeature);
        // verify fields are correct
        List<Property> featureVerifyProps = new ArrayList<>();
        featureVerifyProps.add(new Property("testpropa", "test"));
        featureVerifyProps.add(new Property("testpropa2", "testOverride1"));
        featureVerifyProps.add(new Property("testpropa7", "testOverride3"));
        featureVerifyProps.add(new Property("testpropa3", "testVal3"));
        featureVerifyProps.add(new Property("testpropa4", "testValue2"));
        featureVerifyProps.add(new Property("testpropa5", "test3"));
        featureVerifyProps.add(new Property("testpropa6", "testVal6"));
        Feature checkOut = new Feature(newModelRet3.getId(), featureVerifyProps);
        checkOut.getInheritsFrom().addAll(Arrays.asList(newModelRet3.getId(), newModelRet2.getId(), newModelRet1.getId()));
        verifyFeatureEquals(checkOut, outFeature);
    }

    @Test
    public void testCastFeatureToModel() throws Exception {
        // create default models & features for a ref prop
        Model newModelRetBasic = createBasicModel(openFeatureStore);
        Feature testOutBasic1 = createBasicFeature(openFeatureStore, newModelRetBasic);
        Feature testOutBasic2 = createBasicFeature(openFeatureStore, newModelRetBasic);
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testpropa", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testpropa2", Long.class, Cardinality.SINGLE);
        RefPropertyKey testRefProp1 = new RefPropertyKey("testrefprop1", newModelRetBasic.getId(), Cardinality.SINGLE);
        RefPropertyKey testRefProp2 = new RefPropertyKey("testrefprop2", newModelRetBasic.getId(), Cardinality.SINGLE);
        repo.createProperty(testProp);
        repo.createProperty(testProp2);
        repo.createProperty(testRefProp1);
        repo.createProperty(testRefProp2);
        // create model 1
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testpropa", "test"));
        fproperties.add(new Property("testpropa2"));
        fproperties.add(new Property("testrefprop1"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        Model newModelRet1 = createBasicModel(openFeatureStore, newFeatureSchema, false);
        // create model 2
        List<Property> fproperties2 = new ArrayList<>();
        fproperties2.add(new Property("testpropa", "test2"));
        fproperties2.add(new Property("testrefprop1"));
        fproperties2.add(new Property("testpropa2", 5L));
        fproperties2.add(new Property("testrefprop2", testOutBasic1.getId()));
        FeatureSchema newFeatureSchema2 = new FeatureSchema(fproperties2);
        Model newModelRet2 = createBasicModel(openFeatureStore, newFeatureSchema2, false);
        // create feature
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testpropa2", 2L));
        featureProps.add(new Property("testrefprop1", testOutBasic2.getId()));
        Feature inpFeature = new Feature(newModelRet1.getId(), featureProps);
        Feature outFeature = addAndReadFeature(inpFeature);
        // cast feature to model 2
        Feature outFeatureCasted = castAndReadFeature(outFeature, newModelRet2);
        // verify fields are correct
        List<Property> featureVerifyProps = new ArrayList<>();
        featureVerifyProps.add(new Property("testpropa", "test"));
        featureVerifyProps.add(new Property("testpropa2", 2L));
        featureVerifyProps.add(new Property("testrefprop1", testOutBasic2.getId()));
        featureVerifyProps.add(new Property("testrefprop2", testOutBasic1.getId()));
        Feature checkOut = new Feature(newModelRet2.getId(), featureVerifyProps);
        checkOut.getInheritsFrom().addAll(Arrays.asList(newModelRet2.getId()));
        verifyFeatureEquals(checkOut, outFeatureCasted);
    }

    @Test
    public void testCastFeatureToModelWithPropertyOverrides() throws Exception {
        // create default models & features for a ref prop
        Model newModelRetBasic = createBasicModel(openFeatureStore);
        Feature testOutBasic1 = createBasicFeature(openFeatureStore, newModelRetBasic);
        Feature testOutBasic2 = createBasicFeature(openFeatureStore, newModelRetBasic);
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testpropa", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testpropa2", Long.class, Cardinality.SINGLE);
        RefPropertyKey testRefProp1 = new RefPropertyKey("testrefprop1", newModelRetBasic.getId(), Cardinality.SINGLE);
        RefPropertyKey testRefProp2 = new RefPropertyKey("testrefprop2", newModelRetBasic.getId(), Cardinality.SINGLE);
        repo.createProperty(testProp);
        repo.createProperty(testProp2);
        repo.createProperty(testRefProp1);
        repo.createProperty(testRefProp2);
        // create model 1
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testpropa", "test"));
        fproperties.add(new Property("testpropa2"));
        fproperties.add(new Property("testrefprop1"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        Model newModelRet1 = createBasicModel(openFeatureStore, newFeatureSchema, false);
        // create model 2
        List<Property> fproperties2 = new ArrayList<>();
        fproperties2.add(new Property("testpropa", "test2"));
        fproperties2.add(new Property("testrefprop1"));
        fproperties2.add(new Property("testpropa2", 5L));
        fproperties2.add(new Property("testrefprop2", testOutBasic1.getId()));
        FeatureSchema newFeatureSchema2 = new FeatureSchema(fproperties2);
        Model newModelRet2 = createBasicModel(openFeatureStore, newFeatureSchema2, false);
        // create feature
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testpropa2", 2L));
        featureProps.add(new Property("testrefprop1", testOutBasic2.getId()));
        Feature inpFeature = new Feature(newModelRet1.getId(), featureProps);
        Feature outFeature = addAndReadFeature(inpFeature);
        // create property overrides
        List<Property> featurePropOverrides = new ArrayList<>();
        featurePropOverrides.add(new Property("testpropa2", 7L));
        featurePropOverrides.add(new Property("testrefprop1", testOutBasic1.getId()));
        // cast feature to model 2
        openFeatureStore.getFeatureRepository().castFeatureToModel(outFeature.getId(), newModelRet2.getId(), featurePropOverrides);
        Feature outFeatureCasted = openFeatureStore.getFeatureRepository().readFeature(outFeature.getId());
        // verify fields are correct
        List<Property> featureVerifyProps = new ArrayList<>();
        featureVerifyProps.add(new Property("testpropa", "test2"));
        featureVerifyProps.add(new Property("testpropa2", 7L));
        featureVerifyProps.add(new Property("testrefprop1", testOutBasic1.getId()));
        featureVerifyProps.add(new Property("testrefprop2", testOutBasic1.getId()));
        Feature checkOut = new Feature(newModelRet2.getId(), featureVerifyProps);
        checkOut.getInheritsFrom().addAll(Arrays.asList(newModelRet2.getId()));
        verifyFeatureEquals(checkOut, outFeatureCasted);
    }

    @Test
    public void testInvalidCastFeatureToModel() throws Exception {
        // create default models & features for a ref prop
        Model newModelRetBasic = createBasicModel(openFeatureStore);
        Feature testOutBasic1 = createBasicFeature(openFeatureStore, newModelRetBasic);
        Feature testOutBasic2 = createBasicFeature(openFeatureStore, newModelRetBasic);
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testpropa", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testpropa2", Long.class, Cardinality.SINGLE);
        RefPropertyKey testRefProp1 = new RefPropertyKey("testrefprop1", newModelRetBasic.getId(), Cardinality.SINGLE);
        RefPropertyKey testRefProp2 = new RefPropertyKey("testrefprop2", newModelRetBasic.getId(), Cardinality.SINGLE);
        repo.createProperty(testProp);
        repo.createProperty(testProp2);
        repo.createProperty(testRefProp1);
        repo.createProperty(testRefProp2);
        // create model 1
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testpropa2"));
        fproperties.add(new Property("testrefprop1"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        Model newModelRet1 = createBasicModel(openFeatureStore, newFeatureSchema, false);
        // create model 2
        List<Property> fproperties2 = new ArrayList<>();
        fproperties2.add(new Property("testpropa"));
        fproperties2.add(new Property("testrefprop1"));
        fproperties2.add(new Property("testpropa2", 5L));
        fproperties2.add(new Property("testrefprop2", testOutBasic1.getId()));
        FeatureSchema newFeatureSchema2 = new FeatureSchema(fproperties2);
        Model newModelRet2 = createBasicModel(openFeatureStore, newFeatureSchema2, false);
        // create feature
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testpropa2", 2L));
        featureProps.add(new Property("testrefprop1", testOutBasic2.getId()));
        Feature inpFeature = new Feature(newModelRet1.getId(), featureProps);
        Feature outFeature = addAndReadFeature(inpFeature);
        // cast feature to model 2
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getFeatureRepository().castFeatureToModel(outFeature.getId(), newModelRet2.getId());
        });
    }

    @Test
    public void testCastUsedFeatureToModel() throws Exception {
        // create 2 models
        Model newModelRetBasic = createBasicModel(openFeatureStore);
        Model newModelRetBasic2 = createBasicModel(openFeatureStore, false);
        // create a ref prop to that model
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testRefProp1 = new RefPropertyKey("testrefprop1", newModelRetBasic.getId(), Cardinality.SINGLE);
        repo.createProperty(testRefProp1);
        // create a feature to the above model
        Feature testOutBasic1 = createBasicFeature(openFeatureStore, newModelRetBasic);
        // create a model using that ref prop (so the feature is referenced somehow)
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testrefprop1", testOutBasic1.getId()));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        createBasicModel(openFeatureStore, newFeatureSchema, false);
        // cast feature to second model (it should fail this since it's in use
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getFeatureRepository().castFeatureToModel(testOutBasic1.getId(), newModelRetBasic2.getId());
        });
    }

    @Test
    public void testReadFeatureInvalidObject() throws Exception {
        // setup
        Schema outModelSchema = makeBasicModelSchema(openFeatureStore);
        // read with select
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getFeatureRepository().readFeature(outModelSchema.getId());
        });
    }

    @Test
    public void testAddFeatureWithRefFields() throws Exception {
        // SETUP ADDING MODELS & FEATURES TO REF
        // =====================================
        // create model for refed feature1 & 2
        Model newModelRet1 = createBasicModel(openFeatureStore);
        Model newModelRet2 = createBasicModel(openFeatureStore, false);
        // create features based on these models
        Feature testOut1 = createBasicFeature(openFeatureStore, newModelRet1);
        Feature testOut2 = createBasicFeature(openFeatureStore, newModelRet2);
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprimprop", String.class, Cardinality.SINGLE);
        RefPropertyKey testRefProp = new RefPropertyKey("testrefprop", newModelRet1.getId(), Cardinality.SINGLE);
        RefPropertyKey testRefProp2 = new RefPropertyKey("testrefprop2", newModelRet2.getId(), Cardinality.SINGLE);
        repo.createProperty(testProp);
        repo.createProperty(testRefProp);
        repo.createProperty(testRefProp2);
        // STARTING ACTUAL SETUP & TESTING
        // =====================================
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprimprop"));
        fproperties.add(new Property("testrefprop"));
        fproperties.add(new Property("testrefprop2"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        Model newModelRet = createBasicModel(openFeatureStore, newFeatureSchema, false);
        // create feature
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testprimprop", "test"));
        featureProps.add(new Property("testrefprop", testOut1.getId()));
        featureProps.add(new Property("testrefprop2", testOut2.getId()));
        Feature inpFeature = new Feature(newModelRet.getId(), featureProps);
        Feature outFeature = addAndReadFeature(inpFeature);
        inpFeature.getInheritsFrom().add(newModelRet.getId());
        verifyFeatureEquals(inpFeature, outFeature);
    }

    @Test
    public void testAddFeaturelAllowableFields() throws Exception {
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey<>("testprop", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey<>("testprop3",
                String.class, Cardinality.SET, new HashSet<>(Arrays.asList("testAllow", "testAllow2")));
        for (PrimitivePropertyKey key : Arrays.asList(testProp, testProp2)) {
            repo.createProperty(key);
        }
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop"));
        fproperties.add(new Property("testprop3"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        Model newModelRet = createBasicModel(openFeatureStore, newFeatureSchema);
        // create feature
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testprop", "test"));
        featureProps.add(new Property("testprop3", "testAllow"));
        Feature newFeature = new Feature(newModelRet.getId(), featureProps);
        Feature featureOut = addAndReadFeature(newFeature);
        // verify feature
        newFeature.getInheritsFrom().add(newModelRet.getId());
        verifyFeatureEquals(newFeature, featureOut);
        // attempt create feature with invalid allow value
        List<Property> featureProps2 = new ArrayList<>();
        featureProps2.add(new Property("testprop", "test"));
        featureProps2.add(new Property("testprop3", "testAllow"));
        featureProps2.add(new Property("testprop3", "testAllowNo"));
        Feature newFeature2 = new Feature(newModelRet.getId(), featureProps2);
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getFeatureRepository().addFeature(newFeature2);
        });
    }

    @Test
    public void testReadFeaturesWithSelect() throws Exception {
        // SETUP ADDING MODELS & FEATURES TO REF
        // =====================================
        Model newModelRet1 = createBasicModel(openFeatureStore);
        Feature testOut1 = createBasicFeature(openFeatureStore, newModelRet1);
        // setup test
        Feature newFeature = setupSelectTest(newModelRet1, testOut1);
        Set<String> selectSet = new HashSet<>(Arrays.asList("testprimprop3", "testprimprop", "testrefprop2", "testrefprop3"));
        Feature outFeature = openFeatureStore.getFeatureRepository().readFeature(newFeature.getId(), selectSet);
        // verify outputs
        verifyViewSelect(testOut1, newFeature, outFeature);
    }

    @Test
    public void testSearchFeaturesWithSelect() throws Exception {
        // SETUP ADDING MODELS & FEATURES TO REF
        // =====================================
        Model newModelRet1 = createBasicModel(openFeatureStore);
        Feature testOut1 = createBasicFeature(openFeatureStore, newModelRet1);
        // setup test
        Feature newFeature = setupSelectTest(newModelRet1, testOut1);
        Set<String> selectSet = new HashSet<>(Arrays.asList("testprimprop3", "testprimprop", "testrefprop2", "testrefprop3"));
        // search
        Feature outFeature = openFeatureStore.getFeatureRepository().search(
                (g) -> {
                    return g.V(newFeature.getId());
                }, Optional.of(selectSet)).get(0);
        // verify outputs
        verifyViewSelect(testOut1, newFeature, outFeature);
    }

    private void verifyViewSelect(Feature testOut1, Feature newFeatureCreated, Feature outFeature) {
        List<Property> featurePropsCheck = new ArrayList<>();
        featurePropsCheck.add(new Property("testprimprop", "test1"));
        featurePropsCheck.add(new Property("testprimprop3", "test3"));
        featurePropsCheck.add(new Property("testprimprop3", "test33"));
        featurePropsCheck.add(new Property("testrefprop2", testOut1.getId()));
        featurePropsCheck.add(new Property("testrefprop2", testOut1.getId()));
        featurePropsCheck.add(new Property("testrefprop2", testOut1.getId()));
        featurePropsCheck.add(new Property("testrefprop3", testOut1.getId()));
        newFeatureCreated.getProperties().clear();
        newFeatureCreated.getProperties().addAll(featurePropsCheck);
        verifyFeatureEquals(newFeatureCreated, outFeature);
    }

    private Feature setupSelectTest(Model inpModel, Feature inpFeature) throws Exception {
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprimprop", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testprimprop2", String.class, Cardinality.LIST);
        PrimitivePropertyKey testProp3 = new PrimitivePropertyKey("testprimprop3", String.class, Cardinality.LIST);
        RefPropertyKey testRefProp = new RefPropertyKey("testrefprop", inpModel.getId(), Cardinality.SINGLE);
        RefPropertyKey testRefProp2 = new RefPropertyKey("testrefprop2", inpModel.getId(), Cardinality.LIST);
        RefPropertyKey testRefProp3 = new RefPropertyKey("testrefprop3", inpModel.getId(), Cardinality.SINGLE);
        repo.createProperty(testProp);
        repo.createProperty(testProp2);
        repo.createProperty(testProp3);
        repo.createProperty(testRefProp);
        repo.createProperty(testRefProp2);
        repo.createProperty(testRefProp3);
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprimprop"));
        fproperties.add(new Property("testprimprop2"));
        fproperties.add(new Property("testprimprop3"));
        fproperties.add(new Property("testrefprop"));
        fproperties.add(new Property("testrefprop2"));
        fproperties.add(new Property("testrefprop3"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        Model newModelRet = createBasicModel(openFeatureStore, newFeatureSchema, false);
        // create feature
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testprimprop", "test1"));
        featureProps.add(new Property("testprimprop2", "test2"));
        featureProps.add(new Property("testprimprop2", "test22s"));
        featureProps.add(new Property("testprimprop3", "test3"));
        featureProps.add(new Property("testprimprop3", "test33"));
        featureProps.add(new Property("testrefprop", inpFeature.getId()));
        featureProps.add(new Property("testrefprop2", inpFeature.getId()));
        featureProps.add(new Property("testrefprop2", inpFeature.getId()));
        featureProps.add(new Property("testrefprop2", inpFeature.getId()));
        featureProps.add(new Property("testrefprop3", inpFeature.getId()));
        Feature newFeature = new Feature(newModelRet.getId(), featureProps);
        return addAndReadFeature(newFeature);
    }

    @Test
    public void testAddFeatureWithDefaultRefFields() throws Exception {
        // SETUP ADDING MODELS & FEATURES TO REF
        // =====================================
        // create model for refed feature1 & 2
        Model newModelRet1 = createBasicModel(openFeatureStore);
        Model newModelRet2 = createBasicModel(openFeatureStore, false);
        // create features based on these models
        Feature testOut1 = createBasicFeature(openFeatureStore, newModelRet1);
        Feature testOut2 = createBasicFeature(openFeatureStore, newModelRet2);
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprimprop", String.class, Cardinality.SINGLE);
        RefPropertyKey testRefProp = new RefPropertyKey("testrefprop", newModelRet1.getId(), Cardinality.SINGLE);
        RefPropertyKey testRefProp2 = new RefPropertyKey("testrefprop2", newModelRet2.getId(), Cardinality.SINGLE);
        repo.createProperty(testProp);
        repo.createProperty(testRefProp);
        repo.createProperty(testRefProp2);
        // STARTING ACTUAL SETUP & TESTING
        // =====================================
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprimprop", "test"));
        fproperties.add(new Property("testrefprop", testOut1.getId()));
        fproperties.add(new Property("testrefprop2", testOut2.getId()));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        Model newModelRet = createBasicModel(openFeatureStore, newFeatureSchema, false);
        // create feature
        List<Property> featureProps = new ArrayList<>();
        Feature inpFeature = new Feature(newModelRet.getId(), featureProps);
        Feature outFeature = addAndReadFeature(inpFeature);
        inpFeature.getInheritsFrom().add(newModelRet.getId());
        inpFeature.getProperties().add(new Property("testprimprop", "test"));
        inpFeature.getProperties().add(new Property("testrefprop", testOut1.getId()));
        inpFeature.getProperties().add(new Property("testrefprop2", testOut2.getId()));
        verifyFeatureEquals(inpFeature, outFeature);
    }

    @Test
    public void testAddFeatureWithDeepSubtypeRefField() throws Exception {
        // create supertype model
        Model newModelRet1 = createBasicModel(openFeatureStore);
        // create subtype moel
        Model newModelRet2 = createBasicModel(openFeatureStore, new FeatureSchema(new ArrayList<>()), new HashSet(Arrays.asList(newModelRet1.getId())), false);
        Feature testOut1 = createBasicFeature(openFeatureStore, newModelRet2);
        Feature testOut2 = createBasicFeature(openFeatureStore, newModelRet2);
        // create model used for the test's feature (refs supertype model)
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testProp = new RefPropertyKey("testrefprop", newModelRet1.getId(), Cardinality.SINGLE);
        repo.createProperty(testProp);
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testrefprop", testOut1.getId()));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        Model outModel = createBasicModel(openFeatureStore, newFeatureSchema, false);
        // create feature refing the subtype model & set it to see if it works
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testrefprop", testOut2.getId()));
        Feature inpFeature = new Feature(outModel.getId(), featureProps);
        Feature fout = addAndReadFeature(inpFeature);
        inpFeature.getInheritsFrom().add(outModel.getId());
        verifyFeatureEquals(inpFeature, fout);
    }

    @Test
    public void testAddFeatureWithSingleCardinalityReference() throws Exception {
        Model newModelRet1 = createBasicModel(openFeatureStore);
        Feature testOut1 = createBasicFeature(openFeatureStore, newModelRet1);
        Feature testOut2 = createBasicFeature(openFeatureStore, newModelRet1);
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testRefProp = new RefPropertyKey("testrefprop", newModelRet1.getId(), Cardinality.SINGLE);
        repo.createProperty(testRefProp);
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testrefprop"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        Model newModelRet = createBasicModel(openFeatureStore, newFeatureSchema, false);
        // create feature
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testrefprop", testOut1.getId()));
        featureProps.add(new Property("testrefprop", testOut2.getId()));
        Feature inpFeature = new Feature(newModelRet.getId(), featureProps);
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getFeatureRepository().addFeature(inpFeature);
        });
    }

    @Test
    public void testAddFeatureWithSetCardinalityReference() throws Exception {
        Model newModelRet1 = createBasicModel(openFeatureStore);
        Feature testOut1 = createBasicFeature(openFeatureStore, newModelRet1);
        Feature testOut2 = createBasicFeature(openFeatureStore, newModelRet1);
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testRefProp = new RefPropertyKey("testrefprop", newModelRet1.getId(), Cardinality.SET);
        repo.createProperty(testRefProp);
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testrefprop"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        Model newModelRet = createBasicModel(openFeatureStore, newFeatureSchema, false);
        // create feature
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testrefprop", testOut1.getId()));
        featureProps.add(new Property("testrefprop", testOut2.getId()));
        featureProps.add(new Property("testrefprop", testOut1.getId()));
        Feature inpFeature = new Feature(newModelRet.getId(), featureProps);
        Feature fout = addAndReadFeature(inpFeature);
        assertEquals(2, fout.getProperties().size());
    }

    @Test
    public void testAddFeatureWithListCardinalityReference() throws Exception {
        Model newModelRet1 = createBasicModel(openFeatureStore);
        Feature testOut1 = createBasicFeature(openFeatureStore, newModelRet1);
        Feature testOut2 = createBasicFeature(openFeatureStore, newModelRet1);
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testRefProp = new RefPropertyKey("testrefprop", newModelRet1.getId(), Cardinality.LIST);
        repo.createProperty(testRefProp);
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testrefprop"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        Model newModelRet = createBasicModel(openFeatureStore, newFeatureSchema, false);
        // create feature
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testrefprop", testOut1.getId()));
        featureProps.add(new Property("testrefprop", testOut2.getId()));
        featureProps.add(new Property("testrefprop", testOut1.getId()));
        Feature inpFeature = new Feature(newModelRet.getId(), featureProps);
        Feature fout = addAndReadFeature(inpFeature);
        assertEquals(3, fout.getProperties().size());
    }

    @Test
    public void testDeleteFeature() throws Exception {
        Feature basicFeature = createBasicFeature(openFeatureStore);
        // verify read works
        openFeatureStore.getFeatureRepository().readFeature(basicFeature.getId());
        // exec delete
        openFeatureStore.getFeatureRepository().deleteFeature(basicFeature.getId());
        // verify read fails
        assertThrows(VertexNotFoundException.class, () -> {
            openFeatureStore.getFeatureRepository().readFeature(basicFeature.getId());
        });
    }

    @Test
    public void testDeleteRefedFeatureInModelSchema() throws Exception {
        // create model for refed feature1 & 2
        Model newModelRet1 = createBasicModel(openFeatureStore);
        Feature testFeatureOut1 = createBasicFeature(openFeatureStore, newModelRet1);
        // make model schema
        RefPropertyKey testRefProp = new RefPropertyKey("testrefprop", newModelRet1.getId(), Cardinality.SINGLE);
        openFeatureStore.getPropertyRepository().createProperty(testRefProp);
        ModelSchemaRepository modelSchemaRepo = openFeatureStore.getModelSchemaRepository();
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testrefprop", testFeatureOut1.getId()));
        ModelSchema newSchema = new ModelSchema(properties);
        modelSchemaRepo.createSchema(newSchema);
        // verify delete fails
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getFeatureRepository().deleteFeature(testFeatureOut1.getId());
        });
    }

    @Test
    public void testDeleteRefedFeatureInFeatureSchema() throws Exception {
        // SETUP ADDING MODELS & FEATURES TO REF
        // =====================================
        // create model for refed feature
        Model newModelRet1 = createBasicModel(openFeatureStore);
        // create feature based on the model
        Feature testOut1 = createBasicFeature(openFeatureStore, newModelRet1);
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testRefProp = new RefPropertyKey("testrefprop", newModelRet1.getId(), Cardinality.SINGLE);
        repo.createProperty(testRefProp);
        // STARTING ACTUAL SETUP & TESTING
        // =====================================
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testrefprop", testOut1.getId()));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        createBasicModel(openFeatureStore, newFeatureSchema, false);
        // verify delete fails
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getFeatureRepository().deleteFeature(testOut1.getId());
        });
    }

    @Test
    public void testDeleteRefedFeatureInModelProp() throws Exception {
        // create default model & feature
        Model newModelRet = createBasicModel(openFeatureStore);
        Feature newFeature = createBasicFeature(openFeatureStore, newModelRet);
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testProp2 = new RefPropertyKey("testModelpropRef", newModelRet.getId(), Cardinality.SINGLE);
        repo.createProperty(testProp2);
        // create model schema
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testModelpropRef"));
        ModelSchema newModelSchema = new ModelSchema(properties);
        String modelSchemaId = openFeatureStore.getModelSchemaRepository().createSchema(newModelSchema);
        ModelSchema newModelSchemaOut = openFeatureStore.getModelSchemaRepository().readSchema(modelSchemaId);
        // build model props
        List<Property> modelProps = new ArrayList<>();
        modelProps.add(new Property("testModelpropRef", newFeature.getId()));
        // build model
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop", "test"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        Model inpModel3 = new Model(new HashSet<>(), modelProps, newModelSchemaOut.getId(), newFeatureSchema);
        openFeatureStore.getModelRepository().createModel(inpModel3);
        // verify delete fails
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getFeatureRepository().deleteFeature(newFeature.getId());
        });
    }

    @Test
    public void testDeleteRefedFeatureInFeatureProp() throws Exception {
        // SETUP ADDING MODELS & FEATURES TO REF
        // =====================================
        // create model for refed feature1 
        Model newModelRet1 = createBasicModel(openFeatureStore);
        // create features based on these models
        Feature testOut1 = createBasicFeature(openFeatureStore, newModelRet1);
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testRefProp = new RefPropertyKey("testrefprop", newModelRet1.getId(), Cardinality.SINGLE);
        repo.createProperty(testRefProp);
        // STARTING ACTUAL SETUP & TESTING
        // =====================================
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testrefprop"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        Model newModelRet = createBasicModel(openFeatureStore, newFeatureSchema, false);
        // create feature
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testrefprop", testOut1.getId()));
        Feature inpFeature = new Feature(newModelRet.getId(), featureProps);
        openFeatureStore.getFeatureRepository().addFeature(inpFeature);
        // verify delete fails
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getFeatureRepository().deleteFeature(testOut1.getId());
        });
    }

    @Test
    public void testModifyFeature() throws Exception {
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testprop2", Long.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp3 = new PrimitivePropertyKey("testprop3", String.class, Cardinality.SINGLE);
        for (PrimitivePropertyKey key : Arrays.asList(testProp, testProp2, testProp3)) {
            repo.createProperty(key);
        }
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop", "test"));
        fproperties.add(new Property("testprop2", 2L));
        fproperties.add(new Property("testprop3", "test"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        Model newModelRet = createBasicModel(openFeatureStore, newFeatureSchema);
        // create feature
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testprop", "test"));
        featureProps.add(new Property("testprop2", 2L));
        featureProps.add(new Property("testprop3", "test2"));
        Feature inpFeature = new Feature(newModelRet.getId(), featureProps);
        Feature outFeature = addAndReadFeature(inpFeature);
        // modify feature
        featureProps = new ArrayList<>();
        featureProps.add(new Property("testprop", "test2"));
        featureProps.add(new Property("testprop2", 5L));
        openFeatureStore.getFeatureRepository().updateFeature(outFeature.getId(), featureProps);
        Feature outModified = openFeatureStore.getFeatureRepository().readFeature(outFeature.getId());
        // verify feature modified
        Feature modifiedFeature = new Feature(newModelRet.getId(), featureProps);
        modifiedFeature.getProperties().add(new Property("testprop3", "test"));
        modifiedFeature.getInheritsFrom().add(newModelRet.getId());
        verifyFeatureEquals(modifiedFeature, outModified);
    }

    @Test
    public void testFeatureSearch() throws Exception {
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testprop2", Long.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp3 = new PrimitivePropertyKey("testprop3", String.class, Cardinality.SINGLE);
        for (PrimitivePropertyKey key : Arrays.asList(testProp, testProp2, testProp3)) {
            repo.createProperty(key);
        }
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop", "test"));
        fproperties.add(new Property("testprop2", 2L));
        fproperties.add(new Property("testprop3"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        Model newModelRet = createBasicModel(openFeatureStore, newFeatureSchema);
        // create features
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testprop", "test"));
        featureProps.add(new Property("testprop2", 2L));
        featureProps.add(new Property("testprop3", "test"));
        Feature inpFeature = new Feature(newModelRet.getId(), featureProps);
        Feature outFeature1 = addAndReadFeature(inpFeature);
        List<Property> featureProps2 = new ArrayList<>();
        featureProps2.add(new Property("testprop", "test2"));
        featureProps2.add(new Property("testprop2", 3L));
        featureProps2.add(new Property("testprop3", "test2"));
        Feature inpFeature2 = new Feature(newModelRet.getId(), featureProps2);
        Feature outFeature2 = addAndReadFeature(inpFeature2);
        // search
        List<Feature> outFeatures = openFeatureStore.getFeatureRepository().search(
                (g) -> {
                    return g.V().order().by("testprop2");
                }
        );
        verifyFeatureEquals(outFeature1, outFeatures.get(0));
        verifyFeatureEquals(outFeature2, outFeatures.get(1));
    }

    @Test
    public void testFeatureModificationDuringSearch() throws Exception {
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testprop2", Long.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp3 = new PrimitivePropertyKey("testprop3", String.class, Cardinality.SINGLE);
        for (PrimitivePropertyKey key : Arrays.asList(testProp, testProp2, testProp3)) {
            repo.createProperty(key);
        }
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop", "test"));
        fproperties.add(new Property("testprop2", 2L));
        fproperties.add(new Property("testprop3"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        Model newModelRet = createBasicModel(openFeatureStore, newFeatureSchema);
        // create feature
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testprop", "test"));
        featureProps.add(new Property("testprop2", 2L));
        featureProps.add(new Property("testprop3", "test"));
        Feature inpFeature = new Feature(newModelRet.getId(), featureProps);
        Feature outFeature1 = addAndReadFeature(inpFeature);
        // search
        assertThrows(VerificationException.class, () -> {
            openFeatureStore.getFeatureRepository().search(
                    (g) -> {
                        try ( Transaction tx = g.tx()) {
                            g.V(outFeature1.getId()).property("testprop", "test2").next();
                            tx.commit();
                        }
                        return g.V(newModelRet.getId());
                    }
            );
        });
    }

    @Test
    public void testFeatureSearchWithStrategy() throws Exception {
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testprop2", Long.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp3 = new PrimitivePropertyKey("testprop3", String.class, Cardinality.SINGLE);
        for (PrimitivePropertyKey key : Arrays.asList(testProp, testProp2, testProp3)) {
            repo.createProperty(key);
        }
        // create feature schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprop", "test"));
        fproperties.add(new Property("testprop2", 2L));
        fproperties.add(new Property("testprop3"));
        FeatureSchema newFeatureSchema = new FeatureSchema(fproperties);
        // create model
        Model newModelRet = createBasicModel(openFeatureStore, newFeatureSchema);
        // create features
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testprop", "test"));
        featureProps.add(new Property("testprop2", 2L));
        featureProps.add(new Property("testprop3", "test"));
        Feature inpFeature = new Feature(newModelRet.getId(), featureProps);
        openFeatureStore.getFeatureRepository().addFeature(inpFeature);
        List<Property> featureProps2 = new ArrayList<>();
        featureProps2.add(new Property("testprop", "test2"));
        featureProps2.add(new Property("testprop2", 3L));
        featureProps2.add(new Property("testprop3", "test2"));
        Feature inpFeature2 = new Feature(newModelRet.getId(), featureProps2);
        Feature outFeature2 = addAndReadFeature(inpFeature2);
        // search
        List<Feature> outFeatures = openFeatureStore.getFeatureRepository().search(
                (g) -> {
                    return g.withStrategies(SubgraphStrategy.build()
                            .vertices(has("testprop2", 3L))
                            .create()).V();
                }
        );
        assertEquals(1, outFeatures.size());
        verifyFeatureEquals(outFeature2, outFeatures.get(0));
    }

    @Test
    public void testModelLockDuringFeatureCreation() throws Exception {
        // create model for refed feature
        Model newModelRet = createBasicModel(openFeatureStore);
        // create custom open search with mocks for locks
        ArgumentCaptor<Set<String>> lockAcquireCaptor = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<Set<String>> lockReleaseCaptor = ArgumentCaptor.forClass(Set.class);
        ResourceLock mockLock = mock(ResourceLock.class);
        OpenFeatureStore customOpenSearch = getOpenFeatureStore(OFSConfigurationParams.build().setResourceLock(mockLock));
        // create feature
        List<Property> featureProps = new ArrayList<>();
        featureProps.add(new Property("testprop", "test"));
        Feature inpFeature = new Feature(newModelRet.getId(), featureProps);
        // exec
        customOpenSearch.getFeatureRepository().addFeature(inpFeature);
        // verify
        verify(mockLock, times(1)).acquireLocks(lockAcquireCaptor.capture());
        verify(mockLock, times(1)).releaseLocks(lockReleaseCaptor.capture());
        Set<String> expectedLockResources = new HashSet<>();
        expectedLockResources.add(DependencyResource.fromNodeId(OFSType.MODEL, newModelRet.getId()).getResource());
        assertEquals(expectedLockResources, lockAcquireCaptor.getValue());
        assertEquals(expectedLockResources, lockReleaseCaptor.getValue());
    }

    @Test
    public void testFeatureLockDuringCast() throws Exception {
        // create model for refed feature
        Model newModelRet = createBasicModel(openFeatureStore);
        Model newModelRet2 = createBasicModel(openFeatureStore, false);
        Feature createFeature = createBasicFeature(openFeatureStore, newModelRet);
        // create custom OFS with mocks for locks
        ArgumentCaptor<Set<String>> lockAcquireCaptor = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<Set<String>> lockReleaseCaptor = ArgumentCaptor.forClass(Set.class);
        ResourceLock mockLock = mock(ResourceLock.class);
        OpenFeatureStore customOpenFeatureStore = getOpenFeatureStore(OFSConfigurationParams.build().setResourceLock(mockLock));
        // exec
        customOpenFeatureStore.getFeatureRepository().castFeatureToModel(createFeature.getId(), newModelRet2.getId());
        // verify
        verify(mockLock, times(2)).acquireLocks(lockAcquireCaptor.capture());
        verify(mockLock, times(2)).releaseLocks(lockReleaseCaptor.capture());
        Set<String> expectedModelLockResources = new HashSet<>();
        expectedModelLockResources.add(DependencyResource.fromNodeId(OFSType.MODEL, newModelRet2.getId()).getResource());
        Set<String> expectedFeatureLockResources = new HashSet<>();
        expectedFeatureLockResources.add(DependencyResource.fromNodeId(OFSType.FEATURE, createFeature.getId()).getResource());
        assertEquals(expectedFeatureLockResources, lockAcquireCaptor.getAllValues().get(0));
        assertEquals(expectedFeatureLockResources, lockReleaseCaptor.getAllValues().get(1));
        assertEquals(expectedModelLockResources, lockAcquireCaptor.getAllValues().get(1));
        assertEquals(expectedModelLockResources, lockReleaseCaptor.getAllValues().get(0));
    }

    @Test
    public void testFeatureLockDuringDelete() throws Exception {
        // create model for refed feature
        Model newModelRet = createBasicModel(openFeatureStore);
        Feature createFeature = createBasicFeature(openFeatureStore, newModelRet);
        // create custom open search with mocks for locks
        ArgumentCaptor<Set<String>> lockAcquireCaptor = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<Set<String>> lockReleaseCaptor = ArgumentCaptor.forClass(Set.class);
        ResourceLock mockLock = mock(ResourceLock.class);
        OpenFeatureStore customOFS = getOpenFeatureStore(OFSConfigurationParams.build().setResourceLock(mockLock));
        // exec
        customOFS.getFeatureRepository().deleteFeature(createFeature.getId());
        // verify
        verify(mockLock, times(1)).acquireLocks(lockAcquireCaptor.capture());
        verify(mockLock, times(1)).releaseLocks(lockReleaseCaptor.capture());
        Set<String> expectedLockResources = new HashSet<>();
        expectedLockResources.add(DependencyResource.fromNodeId(OFSType.FEATURE, createFeature.getId()).getResource());
        assertEquals(expectedLockResources, lockAcquireCaptor.getValue());
        assertEquals(expectedLockResources, lockReleaseCaptor.getValue());
    }

    public static void verifyFeatureEquals(Feature expectedFeature, Feature givenFeature) {
        assertEquals(expectedFeature.getModelId(), givenFeature.getModelId());
        assertEquals(expectedFeature.getInheritsFrom(), givenFeature.getInheritsFrom());
        ModelTest.verifyPropertiesEqual(expectedFeature.getProperties(), givenFeature.getProperties());
    }

    private Feature addAndReadFeature(Feature inpFeature) throws Exception {
        String id = openFeatureStore.getFeatureRepository().addFeature(inpFeature);
        return openFeatureStore.getFeatureRepository().readFeature(id);
    }
    
    private Feature castAndReadFeature(Feature outFeature, Model newModelRet) throws Exception {
        openFeatureStore.getFeatureRepository().castFeatureToModel(outFeature.getId(), newModelRet.getId());
        return openFeatureStore.getFeatureRepository().readFeature(outFeature.getId());
    }

}
