/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.property;

import java.io.IOException;
import org.lwing.ofs.core.OpenFeatureStore;
import org.lwing.ofs.core.api.JGMgntProvider;
import org.lwing.ofs.core.api.config.OFSConfigurationParams;
import org.lwing.ofs.core.api.VertexType;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.exception.InternalKeywordException;
import org.lwing.ofs.core.api.feature.Feature;
import org.lwing.ofs.core.api.state.DependencyResource;
import org.lwing.ofs.core.api.lock.ResourceLock;
import org.lwing.ofs.core.api.model.Model;
import org.lwing.ofs.core.api.property.Cardinality;
import org.lwing.ofs.core.api.property.JProperty;
import org.lwing.ofs.core.api.property.PrimitivePropertyKey;
import org.lwing.ofs.core.api.property.RefPropertyKey;
import org.lwing.ofs.core.api.schema.Schema;
import org.lwing.ofs.core.impl.GraphTest;
import org.lwing.ofs.core.impl.OFSRepository;
import org.lwing.ofs.core.impl.install.OFSInstaller;
import static org.lwing.ofs.core.impl.property.PropertyRepository.PROPERTY_NAME;
import org.lwing.ofs.core.impl.schema.SchemaRepository;
import org.lwing.ofs.core.impl.schema.SchemaTest;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.SchemaViolationException;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.JanusGraphManagement;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.lwing.ofs.core.api.schema.FeatureSchema;
import org.lwing.ofs.core.api.schema.ModelSchema;
import org.lwing.ofs.core.api.schema.ViewSchema;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;
import org.mockito.ArgumentCaptor;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 *
 * @author Lucas Wing
 */
public class PropertyTest extends GraphTest {

    @Test
    public void testCreateProperty() throws GraphIntegrityException, Exception {
        // create props
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey outProp = createAndReadPrimProp(testProp);
        // verify outputs
        verifyPropEquality(testProp, outProp);
    }
    
    @Test
    public void testCreatePrimPropertyWithId() throws GraphIntegrityException, Exception {
        // create props
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testId", "testprop", String.class, Cardinality.SINGLE, new HashSet<>());
        PrimitivePropertyKey outProp = createAndReadPrimProp(testProp);
        // verify outputs
        assertEquals("testId", outProp.getId());
    }
    
    @Test
    public void testCreateRefPropertyWithId() throws GraphIntegrityException, Exception {
        String modelId = createBasicModel(openFeatureStore).getId();
        // create prop
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testProp = new RefPropertyKey("testId", "testpropref", modelId, Cardinality.SINGLE, new HashSet<>());
        RefPropertyKey outProp = createAndReadRefProp(testProp);
        // verify outputs
        assertEquals("testId", outProp.getId());
    }

    @Test
    public void testCreateInternalPrimitiveProperty() throws GraphIntegrityException, Exception {
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("_testprop", String.class, Cardinality.SINGLE);
        // verify outputs
        openFeatureStore.getConfig().setAllowInternalFieldActions(false);
        assertThrows(InternalKeywordException.class, () -> {
            repo.createProperty(testProp);
        });
        openFeatureStore.getConfig().setAllowInternalFieldActions(true);
        repo.createProperty(testProp);
    }

    @Test
    public void testCreateInternalRefProperty() throws GraphIntegrityException, Exception {
        // create setup model
        String modelId = createBasicModel(openFeatureStore).getId();
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testProp = new RefPropertyKey("_testpropref", modelId, Cardinality.SINGLE);
        openFeatureStore.getConfig().setAllowInternalFieldActions(false);
        assertThrows(InternalKeywordException.class, () -> {
            repo.createProperty(testProp);
        });
        openFeatureStore.getConfig().setAllowInternalFieldActions(true);
        repo.createProperty(testProp);
    }

    @Test
    public void testDeleteInternalPrimitiveProperty() throws GraphIntegrityException, Exception {
        // create setup model
        String modelId = createBasicModel(openFeatureStore).getId();
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testProp = new RefPropertyKey("_testpropref", modelId, Cardinality.SINGLE);
        openFeatureStore.getConfig().setAllowInternalFieldActions(true);
        repo.createProperty(testProp);
        openFeatureStore.getConfig().setAllowInternalFieldActions(false);
        // verify outputs
        assertThrows(InternalKeywordException.class, () -> {
            repo.deleteProperty(testProp.getName());
        });
    }

    @Test
    public void testDeleteInternalRefProperty() throws GraphIntegrityException, Exception {
        // create setup model
        String modelId = createBasicModel(openFeatureStore).getId();
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testProp = new RefPropertyKey("_testpropref", modelId, Cardinality.SINGLE);
        openFeatureStore.getConfig().setAllowInternalFieldActions(true);
        repo.createProperty(testProp);
        openFeatureStore.getConfig().setAllowInternalFieldActions(false);
        // verify outputs
        assertThrows(InternalKeywordException.class, () -> {
            repo.deleteProperty(testProp.getName());
        });
    }

    @Test
    public void testCreateRefProperty() throws GraphIntegrityException, Exception {
        // create setup model
        String modelId = createBasicModel(openFeatureStore).getId();
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testProp = new RefPropertyKey("testpropref", modelId, Cardinality.SINGLE);
        RefPropertyKey outProp = createAndReadRefProp(testProp);
        // verify outputs
        verifyPropEquality(testProp, outProp);
    }

    @Test
    public void testCreateRefPropertyAtNonModelNode() throws GraphIntegrityException, Exception {
        // create setup feature
        Feature feature = createBasicFeature(openFeatureStore);
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testProp = new RefPropertyKey("testpropref", feature.getId(), Cardinality.SINGLE);
        // verify outputs
        assertThrows(GraphIntegrityException.class, () -> {
            repo.createProperty(testProp);
        });
    }

    @Test
    public void testCreateUsedPrimitiveProperty() throws GraphIntegrityException, Exception {
        // setup index for testing
        setupOpensearchWithPropNameIndex();
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        repo.createProperty(testProp);
        // attempt to make using prevous same prop
        assertThrows(SchemaViolationException.class, () -> {
            repo.createProperty(testProp);
        });
    }

    @Test
    public void testCreateUsedRefProperty() throws GraphIntegrityException, Exception {
        // setup index for testing
        setupOpensearchWithPropNameIndex();
        // create setup model
        String modelId = createBasicModel(openFeatureStore).getId();
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        // create props
        RefPropertyKey testProp = new RefPropertyKey("testrefprop", modelId, Cardinality.SINGLE);
        repo.createProperty(testProp);
        // attempt to make using prevous same prop
        assertThrows(SchemaViolationException.class, () -> {
            repo.createProperty(testProp);
        });
    }

    @Test
    public void testCreatePrimiviveUsedByRefProperty() throws GraphIntegrityException, Exception {
        // setup index for testing
        setupOpensearchWithPropNameIndex();
        // create setup model
        String modelId = createBasicModel(openFeatureStore).getId();
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testProp = new RefPropertyKey("testrefprop", modelId, Cardinality.SINGLE);
        repo.createProperty(testProp);
        // attempt to make using prevous same prop
        assertThrows(SchemaViolationException.class, () -> {
            PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testrefprop", String.class, Cardinality.SINGLE);
            repo.createProperty(testProp2);
        });
    }

    @Test
    public void testCreateRefUsedByPrimitiveProperty() throws GraphIntegrityException, Exception {
        // setup index for testing
        setupOpensearchWithPropNameIndex();
        // create setup model
        String modelId = createBasicModel(openFeatureStore).getId();
        // attempt to make using previous same prop (created while creating the model)
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        assertThrows(SchemaViolationException.class, () -> {
            RefPropertyKey testProp2 = new RefPropertyKey("testprop", modelId, Cardinality.SINGLE);
            repo.createProperty(testProp2);
        });
    }

    // close out default greaph and setup new one with install run w/ indicies
    private void setupOpensearchWithPropNameIndex() throws GraphIntegrityException, InterruptedException, ExecutionException, InternalKeywordException, IOException {
        teardownTests();
        setupOpenFeatureStore(false);
        openFeatureStore.getOpenFeatureStoreInstaller().install(new HashSet(Arrays.asList(OFSInstaller.InstallIndex.PROP_NAME)));
    }

    @Test
    public void testRectifyPrimitiveProperties() throws GraphIntegrityException, Exception {
        // create setup model
        String modelId = createBasicModel(openFeatureStore).getId();
        // create props to test with
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testprop2", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp3 = new PrimitivePropertyKey("testprop3", String.class, Cardinality.SINGLE);
        repo.createProperty(testProp2);
        repo.createProperty(testProp3);
        // force delete the property record from mgnt for testprop2 to break it
        deletePropertyFromManagement("testprop2");
        // force delete the vertex record from mgnt for testprop3 to break it
        deletePropertyFromGraph("testprop3", VertexType.PRIM_PROPERTY);
        // initiate rectify during create (these creates should work)
        testProp2 = new PrimitivePropertyKey("testprop2", Long.class, Cardinality.SET);
        testProp3 = new PrimitivePropertyKey("testprop3", Long.class, Cardinality.SET);
        PrimitivePropertyKey testProp2Out = createAndReadPrimProp(testProp2);
        PrimitivePropertyKey testProp3Out = createAndReadPrimProp(testProp3);
        // verify created props are new type and new cardinality
        assertEquals(Long.class, testProp2Out.getDataType());
        assertEquals(Cardinality.SET, testProp2Out.getCardinality());
        assertEquals(Long.class, testProp3Out.getDataType());
        assertEquals(Cardinality.SET, testProp3Out.getCardinality());
    }

    @Test
    public void testRectifyRefProperties() throws GraphIntegrityException, Exception {
        // create setup model
        String modelId = createBasicModel(openFeatureStore).getId();
        String modelId2 = createBasicModel(openFeatureStore, false).getId();
        // create props to test with
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testRefProp2 = new RefPropertyKey("testpropref2", modelId, Cardinality.SINGLE);
        RefPropertyKey testRefProp3 = new RefPropertyKey("testpropref3", modelId, Cardinality.SINGLE);
        repo.createProperty(testRefProp2);
        repo.createProperty(testRefProp3);
        // force delete the property record from mgnt for testpropref2 to break it
        deletePropertyFromManagement("testpropref2");
        // force delete the vertex record from mgnt for testpropref3 to break it
        deletePropertyFromGraph("testpropref3", VertexType.REF_PROPERTY);
        // initiate rectify during create (these creates should work)
        testRefProp2 = new RefPropertyKey("testpropref2", modelId2, Cardinality.SET);
        testRefProp3 = new RefPropertyKey("testpropref3", modelId2, Cardinality.SET);
        RefPropertyKey testRefProp2Out = createAndReadRefProp(testRefProp2);
        RefPropertyKey testRefProp3Out = createAndReadRefProp(testRefProp3);
        // verify created props are new mdoel and new cardinality
        assertEquals(modelId2, testRefProp2Out.getModelId());
        assertEquals(Cardinality.SET, testRefProp2Out.getCardinality());
        assertEquals(modelId2, testRefProp3Out.getModelId());
        assertEquals(Cardinality.SET, testRefProp3Out.getCardinality());
    }

    private void deletePropertyFromManagement(String property) {
        try ( JGMgntProvider mgnt = new JGMgntProvider(graph.openManagement())) {
            JanusGraphManagement management = mgnt.getMgnt();
            PropertyKey prop = management.getPropertyKey(property);
            mgnt.getMgnt().changeName(prop, "deleteMe");
        }
    }

    private void deletePropertyFromGraph(String property, VertexType type) throws Exception {
        try ( GraphTraversalSource g = graph.traversal()) {
            try ( Transaction tx = g.tx()) {
                Vertex propVertex = g.V().has(OFSRepository.NODE_TYPE_FIELD, type.name())
                        .has(PROPERTY_NAME, property).next();
                g.V(propVertex.id()).outE().drop().iterate();
                g.V(propVertex.id()).drop().iterate();
                tx.commit();
            }
        }
    }

    @Test
    public void testCreateDifferentDatatypeProperty() throws GraphIntegrityException, Exception {
        // create props
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", Geoshape.class, Cardinality.SINGLE);
        PrimitivePropertyKey outProp = createAndReadPrimProp(testProp);
        // verify outputs
        verifyPropEquality(testProp, outProp);
    }

    @Test
    public void testCreateBadDatatypeProperty() throws GraphIntegrityException {
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", PrimitivePropertyKey.class, Cardinality.SINGLE);
        // verify you can't create a prop using a datatype not supported by JanusGraph
        assertThrows(IllegalArgumentException.class, () -> {
            repo.createProperty(testProp);
        });
    }

    @Test
    public void testCreateCardinalitySet() throws GraphIntegrityException, Exception {
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", Geoshape.class, Cardinality.SET);
        PrimitivePropertyKey outProp = createAndReadPrimProp(testProp);
        // verify outputs
        assertEquals(testProp.getCardinality(), outProp.getCardinality());
    }

    @Test
    public void testCreateCardinalityList() throws GraphIntegrityException, Exception {
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", Geoshape.class, Cardinality.LIST);
        PrimitivePropertyKey outProp = createAndReadPrimProp(testProp);
        // verify outputs
        assertEquals(testProp.getCardinality(), outProp.getCardinality());
    }

    @Test
    public void testDeletePrimitveProperty() throws GraphIntegrityException, Exception {
        // create prop
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", Geoshape.class, Cardinality.LIST);
        PrimitivePropertyKey outProp = createAndReadPrimProp(testProp);
        assertTrue(repo.readPrimitiveProperty("testprop").isPresent());
        // delete prop
        repo.deleteProperty(outProp.getName());
        // verify outputs
        assertTrue(repo.readPrimitiveProperty("testprop").isEmpty());
    }

    @Test
    public void testDeleteRefProperty() throws GraphIntegrityException, Exception {
        // create setup model
        String modelId = createBasicModel(openFeatureStore).getId();
        // create prop
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testProp = new RefPropertyKey("testrefprop", modelId, Cardinality.SINGLE);
        RefPropertyKey outProp = createAndReadRefProp(testProp);
        assertTrue(repo.readRefProperty("testrefprop").isPresent());
        // delete prop
        repo.deleteProperty(outProp.getName());
        // verify outputs
        assertTrue(repo.readRefProperty("testrefprop").isEmpty());
    }

    @Test
    public void testReadPrimitiveProperty() throws GraphIntegrityException, Exception {
        // create prop
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        repo.createProperty(testProp);
        // read prop
        PrimitivePropertyKey outProp = repo.readPrimitiveProperty("testprop").get();
        // verify outputs
        verifyPropEquality(testProp, outProp);
    }

    @Test
    public void testReadRefProperty() throws GraphIntegrityException, Exception {
        // create setup model
        String modelId = createBasicModel(openFeatureStore).getId();
        // create prop
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testRefProp = new RefPropertyKey("testpropref", modelId, Cardinality.SINGLE);
        repo.createProperty(testRefProp);
        // read prop
        RefPropertyKey outProp = repo.readRefProperty("testpropref").get();
        // verify outputs
        verifyPropEquality(testRefProp, outProp);
    }

    @Test
    public void testReadRefFromPrimitive() throws GraphIntegrityException, Exception {
        // create setup model
        String modelId = createBasicModel(openFeatureStore).getId();
        // create ref prop
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testRefProp = new RefPropertyKey("testpropref", modelId, Cardinality.SINGLE);
        repo.createProperty(testRefProp);
        // read prop
        assertTrue(repo.readPrimitiveProperty("testpropref").isEmpty());
    }
    
    @Test
    public void testContainsProperty() throws GraphIntegrityException, Exception {
        // create prop
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        String modelId = createBasicModel(openFeatureStore).getId();
        RefPropertyKey testRefProp = new RefPropertyKey("testpropref", modelId, Cardinality.SINGLE);
        repo.createProperty(testRefProp);
        // verify outputs
        assertTrue(repo.containsProperty("testprop"));
        assertTrue(repo.containsProperty("testpropref"));
    }
    
    @Test
    public void testDoesNotContainProperty() throws GraphIntegrityException, Exception {
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        assertFalse(repo.containsProperty("testprop"));
        assertFalse(repo.containsProperty("testpropref"));
    }

    @Test
    public void testListProperties() throws GraphIntegrityException, Exception {
        // create props to list
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        List<JProperty> outList1 = repo.listProperties();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        repo.createProperty(testProp);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testprop2", String.class, Cardinality.SINGLE);
        repo.createProperty(testProp2);
        // list props
        List<JProperty> outList = repo.listProperties();
        // verify outputs
        assertEquals(2, outList.size() - outList1.size());
        Set<String> outSet = outList.stream().map(j -> j.getName()).collect(Collectors.toSet());
        assertTrue(outSet.contains("testprop"));
        assertTrue(outSet.contains("testprop2"));
    }

    @Test
    public void testDeleteUsedModelSchemaPrimitiveProperty() throws Exception {
        verifyDeleteUsedSchemaPrimProperty("testModelprop");
    }

    @Test
    public void testDeleteUsedFeatureSchemaPrimitiveProperty() throws Exception {
        verifyDeleteUsedSchemaPrimProperty("testprop");
    }

    private void verifyDeleteUsedSchemaPrimProperty(String propName) throws Exception {
        // create feature prop
        Model outModel = createBasicModel(openFeatureStore);
        // attempt delete
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        assertThrows(GraphIntegrityException.class, () -> {
            repo.deleteProperty(propName);
        });
        openFeatureStore.getModelRepository().deleteModel(outModel.getId());
        openFeatureStore.getModelSchemaRepository().deleteSchema(outModel.getModelSchemaId());
        assertTrue(repo.deleteProperty(propName) != null);
    }
    
     @Test
    public void testDeleteUsedViewSchemaRefProperty() throws Exception {
        ViewSchema newSchema = new ViewSchema(new HashSet<>(Arrays.asList("testschemarefprop")));
        verifyPropUsedInSchemaRefWontDelete(openFeatureStore.getViewSchemaRepository(), newSchema, OFSType.VIEW_SCHEMA);
    }

    @Test
    public void testDeleteUsedFeatureSchemaRefProperty() throws Exception {
        FeatureSchema newSchema = new FeatureSchema(new HashSet<>(Arrays.asList("testschemarefprop")));
        verifyPropUsedInSchemaRefWontDelete(openFeatureStore.getFeatureSchemaRepository(), newSchema, OFSType.FEATURE_SCHEMA);
    }

    @Test
    public void testDeleteUsedModelSchemaRefProperty() throws Exception {
        ModelSchema newSchema = new ModelSchema(new HashSet<>(Arrays.asList("testschemarefprop")));
        verifyPropUsedInSchemaRefWontDelete(openFeatureStore.getModelSchemaRepository(), newSchema, OFSType.MODEL_SCHEMA);
    }

    private void verifyPropUsedInSchemaRefWontDelete(SchemaRepository repo, Schema newSchema, OFSType type) throws Exception {
        OpenFeatureStore search = getOpenFeatureStore();
        Model model = createBasicModel(search);
        // create props
        PropertyRepository propRepo = search.getPropertyRepository();
        RefPropertyKey testProp = new RefPropertyKey("testschemarefprop", model.getId(), Cardinality.SINGLE);
        propRepo.createProperty(testProp);
        // build schema
        Schema newSchemaOut = SchemaTest.createSchema(repo, newSchema);
        assertThrows(GraphIntegrityException.class, () -> {
            propRepo.deleteProperty("testschemarefprop");
        });
        // verify delete works after schema removal
        SchemaTest.deleteSchema(repo, newSchemaOut.getId(), type);
        assertTrue(propRepo.deleteProperty("testschemarefprop") != null);
    }

    @Test
    public void testStringPropAllowableValues() throws Exception {
        PrimitivePropertyKey testStringProp = new PrimitivePropertyKey<>("testStringProp", String.class, Cardinality.SINGLE,
                new HashSet<>(Arrays.asList("testval", "testval2")));
        verifyTypeProp(testStringProp);
    }

    @Test
    public void testCharacterPropAllowableValues() throws Exception {
        PrimitivePropertyKey testProp = new PrimitivePropertyKey<>("testCharacterProp", Character.class, Cardinality.SINGLE,
                new HashSet<>(Arrays.asList('a', 'b')));
        verifyTypeProp(testProp);
    }

    @Test
    public void testBooleanPropAllowableValues() throws Exception {
        PrimitivePropertyKey testProp = new PrimitivePropertyKey<>("testBooleanProp", Boolean.class, Cardinality.SINGLE,
                new HashSet<>(Arrays.asList(false)));
        verifyTypeProp(testProp);
    }

    @Test
    public void testBytePropAllowableValues() throws Exception {
        byte b = 5;
        byte b2 = 8;
        PrimitivePropertyKey testProp = new PrimitivePropertyKey<>("testByteProp", Byte.class, Cardinality.SINGLE,
                new HashSet<>(Arrays.asList(b, b2)));
        verifyTypeProp(testProp);
    }

    @Test
    public void testShortPropAllowableValues() throws Exception {
        short s = 4;
        short s2 = 7;
        short s3 = 8;
        PrimitivePropertyKey testProp = new PrimitivePropertyKey<>("testShortProp", Short.class, Cardinality.SINGLE,
                new HashSet<>(Arrays.asList(s, s2, s3)));
        verifyTypeProp(testProp);
    }

    @Test
    public void testIntPropAllowableValues() throws Exception {
        PrimitivePropertyKey testProp = new PrimitivePropertyKey<>("testIntProp", Integer.class, Cardinality.SINGLE,
                new HashSet<>(Arrays.asList(7)));
        verifyTypeProp(testProp);
    }

    @Test
    public void testLongPropAllowableValues() throws Exception {
        PrimitivePropertyKey testProp = new PrimitivePropertyKey<>("testLongProp", Long.class, Cardinality.SINGLE,
                new HashSet<>(Arrays.asList(7L, 8L)));
        verifyTypeProp(testProp);
    }

    @Test
    public void testFloatPropAllowableValues() throws Exception {
        PrimitivePropertyKey testProp = new PrimitivePropertyKey<>("testFloatProp", Float.class, Cardinality.SINGLE,
                new HashSet<>(Arrays.asList(10f)));
        verifyTypeProp(testProp);
    }

    @Test
    public void testDoublePropAllowableValues() throws Exception {
        PrimitivePropertyKey testProp = new PrimitivePropertyKey<>("testDoubleProp", Double.class, Cardinality.SINGLE,
                new HashSet<>(Arrays.asList(10d, 20d)));
        verifyTypeProp(testProp);
    }

    @Test
    public void testDatePropAllowableValues() throws Exception {
        Date date = new GregorianCalendar(2014, Calendar.FEBRUARY, 11).getTime();
        Date date1 = new GregorianCalendar(2015, Calendar.FEBRUARY, 12).getTime();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey<>("testDoubleProp", Date.class, Cardinality.SINGLE,
                new HashSet<>(Arrays.asList(date, date1)));
        verifyTypeProp(testProp);
    }

    @Test
    public void testGeoshapePropAllowableValues() throws Exception {
        PrimitivePropertyKey testProp = new PrimitivePropertyKey<>("testGeoshapeProp", Geoshape.class, Cardinality.SINGLE,
                new HashSet<>(Arrays.asList(Geoshape.point(37.97, 23.72), Geoshape.box(37.97, 23.72, 38.97, 24.72))));
        verifyTypeProp(testProp);
    }

    @Test
    public void testUuidPropAllowableValues() throws Exception {
        PrimitivePropertyKey testProp = new PrimitivePropertyKey<>("testUuidProp", UUID.class, Cardinality.SINGLE,
                new HashSet<>(Arrays.asList(UUID.fromString("123e4567-e89b-42d3-a456-556642440000"), UUID.fromString("12444567-e89b-42d3-a456-556642440000"))));
        verifyTypeProp(testProp);
    }

    @Test
    public void testRefPropAllowableValues() throws Exception {
        // create setup model
        Model superModel = createBasicModel(openFeatureStore);
        // create setup feature
        Feature feature = createBasicFeature(openFeatureStore, superModel);
        // create prop
        RefPropertyKey testRefProp = new RefPropertyKey("testpropref", superModel.getId(), Cardinality.SINGLE, new HashSet<>(Arrays.asList(feature.getId())));
        // verify
        verifyTypeProp(testRefProp);
    }

    @Test
    public void testRefPropAllowableValuesIncorrectFeatureType() throws Exception {
        // create setup models
        Model superModel = createBasicModel(openFeatureStore);
        Model superModel2 = createBasicModel(openFeatureStore, false);
        // create setup feature
        Feature feature1 = createBasicFeature(openFeatureStore, superModel2);
        // create prop
        RefPropertyKey testRefProp = new RefPropertyKey("testpropref", superModel.getId(), Cardinality.SINGLE, new HashSet<>(Arrays.asList(feature1.getId())));
        // verify
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getPropertyRepository().createProperty(testRefProp);
        });
    }

    @Test
    public void testPrimitiveLockDuringCreation() throws Exception {
        // create custom open search with mocks for locks
        ArgumentCaptor<Set<String>> lockAcquireCaptor = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<Set<String>> lockReleaseCaptor = ArgumentCaptor.forClass(Set.class);
        ResourceLock mockLock = mock(ResourceLock.class);
        OpenFeatureStore customOFS = getOpenFeatureStore(OFSConfigurationParams.build().setResourceLock(mockLock));
        // create ref prop
        String primPropName = "testprop";
        PrimitivePropertyKey testProp = new PrimitivePropertyKey(primPropName, String.class, Cardinality.SINGLE);
        customOFS.getPropertyRepository().createProperty(testProp);
        // verify
        verify(mockLock, times(1)).acquireLocks(lockAcquireCaptor.capture());
        verify(mockLock, times(1)).releaseLocks(lockReleaseCaptor.capture());
        Set<String> expectedLockResources = new HashSet<>();
        expectedLockResources.add(DependencyResource.fromProperty(OFSType.JPROPERTY, primPropName).getResource());
        // the second value is the feature schema lock which I don't care about here, so just chceck the first value
        assertEquals(expectedLockResources, lockAcquireCaptor.getValue());
        assertEquals(expectedLockResources, lockReleaseCaptor.getValue());
    }

    @Test
    public void testRefLockDuringCreation() throws Exception {
        Model newModelRet = createBasicModel(openFeatureStore);
        // create custom OFS with mocks for locks
        ArgumentCaptor<Set<String>> lockAcquireCaptor = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<Set<String>> lockReleaseCaptor = ArgumentCaptor.forClass(Set.class);
        ResourceLock mockLock = mock(ResourceLock.class);
        OpenFeatureStore customOFS = getOpenFeatureStore(OFSConfigurationParams.build().setResourceLock(mockLock));
        // create ref prop
        String refPropName = "testpropref";
        RefPropertyKey testRefProp = new RefPropertyKey(refPropName, newModelRet.getId(), Cardinality.SINGLE);
        customOFS.getPropertyRepository().createProperty(testRefProp);
        // verify
        verify(mockLock, times(1)).acquireLocks(lockAcquireCaptor.capture());
        verify(mockLock, times(1)).releaseLocks(lockReleaseCaptor.capture());
        Set<String> expectedLockResources = new HashSet<>();
        expectedLockResources.add(DependencyResource.fromNodeId(OFSType.MODEL, newModelRet.getId()).getResource());
        expectedLockResources.add(DependencyResource.fromProperty(OFSType.JPROPERTY, refPropName).getResource());
        // the second value is the feature schema lock which I don't care about here, so just chceck the first value
        assertEquals(expectedLockResources, lockAcquireCaptor.getValue());
        assertEquals(expectedLockResources, lockReleaseCaptor.getValue());
    }

    @Test
    public void testRefLockDuringDeletion() throws Exception {
        Model newModelRet = createBasicModel(openFeatureStore);
        // create ref prop
        String refPropName = "testpropref";
        RefPropertyKey testRefProp = new RefPropertyKey(refPropName, newModelRet.getId(), Cardinality.SINGLE);
        RefPropertyKey testRefPropOut = createAndReadRefProp(testRefProp);
        // create custom ofs with mocks for locks
        ArgumentCaptor<Set<String>> lockAcquireCaptor = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<Set<String>> lockReleaseCaptor = ArgumentCaptor.forClass(Set.class);
        ResourceLock mockLock = mock(ResourceLock.class);
        OpenFeatureStore customOFS = getOpenFeatureStore(OFSConfigurationParams.build().setResourceLock(mockLock));
        // delete ref prop
        customOFS.getPropertyRepository().deleteProperty(refPropName);
        // verify
        verify(mockLock, times(2)).acquireLocks(lockAcquireCaptor.capture());
        verify(mockLock, times(2)).releaseLocks(lockReleaseCaptor.capture());
        // this checks the locked properties
        Set<String> expectedLockResources = new HashSet<>();
        expectedLockResources.add(DependencyResource.fromProperty(OFSType.JPROPERTY, refPropName).getResource());
        assertEquals(expectedLockResources, lockAcquireCaptor.getAllValues().get(0));
        assertEquals(expectedLockResources, lockReleaseCaptor.getAllValues().get(1));
    }

    @Test
    public void testPrimPropLockDuringDeletion() throws Exception {
        // create ref prop
        String propName = "testprop";
        PrimitivePropertyKey testProp = new PrimitivePropertyKey(propName, String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testPropOut = createAndReadPrimProp(testProp);
        // create custom ofs with mocks for locks
        ArgumentCaptor<Set<String>> lockAcquireCaptor = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<Set<String>> lockReleaseCaptor = ArgumentCaptor.forClass(Set.class);
        ResourceLock mockLock = mock(ResourceLock.class);
        OpenFeatureStore customOFS = getOpenFeatureStore(OFSConfigurationParams.build().setResourceLock(mockLock));
        // delete ref prop
        customOFS.getPropertyRepository().deleteProperty(propName);
        // verify
        verify(mockLock, times(2)).acquireLocks(lockAcquireCaptor.capture());
        verify(mockLock, times(2)).releaseLocks(lockReleaseCaptor.capture());
        // this checks the locked properties
        Set<String> expectedLockResources = new HashSet<>();
        expectedLockResources.add(DependencyResource.fromProperty(OFSType.JPROPERTY, propName).getResource());
        assertEquals(expectedLockResources, lockAcquireCaptor.getAllValues().get(0));
        assertEquals(expectedLockResources, lockReleaseCaptor.getAllValues().get(1));
    }

    private void verifyTypeProp(PrimitivePropertyKey testInputProp) throws Exception {
        JProperty outPropCreate = createAndReadPrimProp(testInputProp);
        Optional<PrimitivePropertyKey> outPropRead = openFeatureStore.getPropertyRepository().readPrimitiveProperty(testInputProp.getName());
        assertEquals(testInputProp.getAllowableValues(), outPropCreate.getAllowableValues());
        assertEquals(testInputProp.getAllowableValues(), outPropRead.get().getAllowableValues());
    }

    private void verifyTypeProp(RefPropertyKey testInputProp) throws Exception {
        JProperty outPropCreate = createAndReadRefProp(testInputProp);
        Optional<RefPropertyKey> outPropRead = openFeatureStore.getPropertyRepository().readRefProperty(testInputProp.getName());
        assertEquals(testInputProp.getAllowableValues(), outPropCreate.getAllowableValues());
        assertEquals(testInputProp.getAllowableValues(), outPropRead.get().getAllowableValues());
    }

    public static void verifyPropEquality(PrimitivePropertyKey propExpected, PrimitivePropertyKey propGiven) {
        assertEquals(propExpected.getName(), propGiven.getName());
        assertEquals(propExpected.getCardinality(), propGiven.getCardinality());
        assertEquals(propExpected.getDataType(), propGiven.getDataType());
        assertEquals(propExpected.getAllowableValues(), propGiven.getAllowableValues());
    }

    public static void verifyPropEquality(RefPropertyKey propExpected, RefPropertyKey propGiven) {
        assertEquals(propExpected.getName(), propGiven.getName());
        assertEquals(propExpected.getModelId(), propGiven.getModelId());
        assertEquals(propExpected.getAllowableValues(), propGiven.getAllowableValues());
    }
    
    private PrimitivePropertyKey createAndReadPrimProp(PrimitivePropertyKey prop) throws Exception {
        openFeatureStore.getPropertyRepository().createProperty(prop);
        return openFeatureStore.getPropertyRepository().readPrimitiveProperty(prop.getName()).get();
    }
    
    private RefPropertyKey createAndReadRefProp(RefPropertyKey prop) throws Exception {
        openFeatureStore.getPropertyRepository().createProperty(prop);
        return openFeatureStore.getPropertyRepository().readRefProperty(prop.getName()).get();
    }

}
