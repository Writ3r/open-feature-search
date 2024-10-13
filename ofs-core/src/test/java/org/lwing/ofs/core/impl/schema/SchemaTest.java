/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.schema;

import org.lwing.ofs.core.OpenFeatureStore;
import org.lwing.ofs.core.impl.model.ModelSchemaRepository;
import org.lwing.ofs.core.api.config.OFSConfiguration;
import org.lwing.ofs.core.api.config.OFSConfigurationParams;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.exception.InternalKeywordException;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
public class SchemaTest extends GraphTest {

    @Test
    public void testAddSchema() throws GraphIntegrityException, Exception {
        // create props
        PropertyRepository propRepo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        propRepo.createProperty(testProp);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testprop2", Long.class, Cardinality.SINGLE);
        propRepo.createProperty(testProp2);
        PrimitivePropertyKey testProp3 = new PrimitivePropertyKey("testprop3", Long.class, Cardinality.SINGLE);
        propRepo.createProperty(testProp3);
        PrimitivePropertyKey testProp4 = new PrimitivePropertyKey("testprop4", String.class, Cardinality.SET);
        propRepo.createProperty(testProp4);
        // build props map
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testprop", "test"));
        properties.add(new Property("testprop2", 2L));
        properties.add(new Property("testprop4", "test1"));
        properties.add(new Property("testprop4", "test2"));
        Set<String> propKeys = getPropKeysFromProps(properties);
        propKeys.add("testprop3");
        // build schema
        ModelSchema newSchema = new ModelSchema(propKeys, properties);
        Schema out = createAndReadSchema(newSchema, openFeatureStore.getModelSchemaRepository());
        // verify outputs
        verifySchemaEquals(newSchema, out);
    }
    
    @Test
    public void testAdSchemaCustId() throws Exception {
        // create schema
        ModelSchemaRepository modelSchemaRepo = openFeatureStore.getModelSchemaRepository();
        ModelSchema newSchema = new ModelSchema("testId", new HashSet<>(), Arrays.asList());
        ModelSchema outSchema = createAndReadSchema(newSchema, modelSchemaRepo);
        // verify
        assertEquals("testId", outSchema.getId());
    }

    @Test
    public void testAddModelSchemaWithReferences() throws Exception {
        ModelSchema newSchema = new ModelSchema(new HashSet(Arrays.asList("testrefprop")));
        addSchemaWithReference(openFeatureStore.getModelSchemaRepository(), newSchema);
    }

    @Test
    public void testAddFeatureSchemaWithReferences() throws Exception {
        FeatureSchema newSchema = new FeatureSchema(new HashSet(Arrays.asList("testrefprop")));
        addSchemaWithReference(openFeatureStore.getFeatureSchemaRepository(), newSchema);
    }

    @Test
    public void testAddViewSchemaWithReferences() throws Exception {
        ViewSchema newSchema = new ViewSchema(new HashSet(Arrays.asList("testrefprop")));
        addSchemaWithReference(openFeatureStore.getViewSchemaRepository(), newSchema);
    }

    private void addSchemaWithReference(SchemaRepository repo, Schema newSchema) throws Exception {
        // create setup model
        Model model = createBasicModel(openFeatureStore);
        // create props
        PropertyRepository propRepo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testProp = new RefPropertyKey("testrefprop", model.getId(), Cardinality.SINGLE);
        propRepo.createProperty(testProp);
        // build schema
        Schema out = createAndReadSchema(newSchema, repo);
        // verify outputs
        verifySchemaEquals(newSchema, out);
    }

    @Test
    public void addSchemaMultiPropWithSingleCardinalityReference() throws Exception {
        // create setup models/features
        Model model = createBasicModel(openFeatureStore);
        Feature newFeature = createBasicFeature(openFeatureStore, model);
        Feature newFeature2 = createBasicFeature(openFeatureStore, model);
        // create props
        PropertyRepository propRepo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testProp = new RefPropertyKey("testrefprop", model.getId(), Cardinality.SINGLE);
        propRepo.createProperty(testProp);
        // build schema
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testrefprop", newFeature2.getId()));
        properties.add(new Property("testrefprop", newFeature.getId()));
        ModelSchema newSchema = new ModelSchema(properties);
        // verify outputs
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getModelSchemaRepository().createSchema(newSchema);
        });
    }

    @Test
    public void addSchemaWithSetCardinalityReference() throws Exception {
        // create setup models/features
        Model model = createBasicModel(openFeatureStore);
        Feature newFeature = createBasicFeature(openFeatureStore, model);
        Feature newFeature2 = createBasicFeature(openFeatureStore, model);
        // create props
        PropertyRepository propRepo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testProp = new RefPropertyKey("testrefprop", model.getId(), Cardinality.SET);
        propRepo.createProperty(testProp);
        // build props map
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testrefprop", newFeature2.getId()));
        properties.add(new Property("testrefprop", newFeature.getId()));
        properties.add(new Property("testrefprop", newFeature2.getId()));
        // build schema
        ModelSchema newSchema = new ModelSchema(properties);
        Schema out = createAndReadSchema(newSchema, openFeatureStore.getModelSchemaRepository());
        // verify outputs
        assertEquals(2, out.getDefaultProperties().size());
    }

    @Test
    public void addSchemaWithListCardinalityReference() throws Exception {
        // create setup models/features
        Model model = createBasicModel(openFeatureStore);
        Feature newFeature = createBasicFeature(openFeatureStore, model);
        Feature newFeature2 = createBasicFeature(openFeatureStore, model);
        // create props
        PropertyRepository propRepo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testProp = new RefPropertyKey("testrefprop", model.getId(), Cardinality.LIST);
        propRepo.createProperty(testProp);
        // build props map
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testrefprop", newFeature2.getId()));
        properties.add(new Property("testrefprop", newFeature.getId()));
        properties.add(new Property("testrefprop", newFeature2.getId()));
        // build schema
        ModelSchema newSchema = new ModelSchema(properties);
        Schema out = createAndReadSchema(newSchema, openFeatureStore.getModelSchemaRepository());
        // verify outputs
        assertEquals(3, out.getDefaultProperties().size());
    }

    @Test
    public void testDeleteModelSchemaInUse() throws Exception {
        // create model prop
        Model model = createBasicModel(openFeatureStore);
        // verify outputs
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getModelSchemaRepository().deleteSchema(model.getModelSchemaId());
        });
    }

    @Test
    public void testDeleteFeatureSchemaInUse() throws Exception {
        // create model prop
        Model model = createBasicModel(openFeatureStore);
        // verify outputs
        assertThrows(GraphIntegrityException.class, () -> {
            SchemaTest.deleteSchema(openFeatureStore.getFeatureSchemaRepository(), model.getFeatureSchema().getId(), OFSType.FEATURE_SCHEMA);
        });
    }

    @Test
    public void testAddModelSchemaWithReferenceDefaults() throws Exception {
        // create schema
        List<Property> properties = setupAddSchemaWithReferenceDefaults();
        ModelSchema newSchema = new ModelSchema(properties);
        Schema out = createAndReadSchema(newSchema, openFeatureStore.getModelSchemaRepository());
        verifySchemaEquals(newSchema, out);
    }

    @Test
    public void testAddFeatureSchemaWithReferenceDefaults() throws Exception {
        List<Property> properties = setupAddSchemaWithReferenceDefaults();
        FeatureSchema newSchema = new FeatureSchema(properties);
        Schema out = createAndReadSchema(newSchema, openFeatureStore.getFeatureSchemaRepository());
        verifySchemaEquals(newSchema, out);
    }

    @Test
    public void testAddViewSchemaWithReferenceDefaults() throws Exception {
        List<Property> properties = setupAddSchemaWithReferenceDefaults();
        ViewSchema newSchema = new ViewSchema(properties);
        Schema out = createAndReadSchema(newSchema, openFeatureStore.getViewSchemaRepository());
        verifySchemaEquals(newSchema, out);
    }

    private List<Property> setupAddSchemaWithReferenceDefaults() throws Exception {
        // create setup models/features
        Model model = createBasicModel(openFeatureStore);
        Feature feature = createBasicFeature(openFeatureStore, model);
        Feature feature2 = createBasicFeature(openFeatureStore, model);
        Feature feature3 = createBasicFeature(openFeatureStore, model);
        // create props
        PropertyRepository propRepo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testProp = new RefPropertyKey("testrefprop", model.getId(), Cardinality.LIST);
        RefPropertyKey testProp2 = new RefPropertyKey("testrefprop2", model.getId(), Cardinality.SINGLE);
        PrimitivePropertyKey testPrimProp = new PrimitivePropertyKey("testpropPrim", String.class, Cardinality.SINGLE);
        propRepo.createProperty(testProp);
        propRepo.createProperty(testProp2);
        propRepo.createProperty(testPrimProp);
        // create schema props
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testrefprop", feature.getId()));
        properties.add(new Property("testrefprop", feature2.getId()));
        properties.add(new Property("testrefprop2", feature3.getId()));
        properties.add(new Property("testpropPrim", "test"));
        return properties;
    }

    @Test
    public void testAddModelSchemaAllowableFields() throws Exception {
        // create good schema
        List<Property> schemaProps = new ArrayList();
        schemaProps.add(new Property("testprop", "test"));
        schemaProps.add(new Property("testprop3", "testAllow"));
        ModelSchema newSchema = new ModelSchema(schemaProps);
        // create bad schema
        List<Property> schemaProps1 = new ArrayList();
        schemaProps1.add(new Property("testprop", "test"));
        schemaProps1.add(new Property("testprop3", "testAllowNO"));
        schemaProps1.add(new Property("testprop3", "testAllow"));
        ModelSchema newSchema1 = new ModelSchema(schemaProps1);
        testAddSchemaAllowableFields(openFeatureStore.getModelSchemaRepository(), newSchema, newSchema1);
    }

    @Test
    public void testAddFeatureSchemaAllowableFields() throws Exception {
        // create good schema
        List<Property> schemaProps = new ArrayList();
        schemaProps.add(new Property("testprop", "test"));
        schemaProps.add(new Property("testprop3", "testAllow"));
        FeatureSchema newSchema = new FeatureSchema(schemaProps);
        // create bad schema
        List<Property> schemaProps1 = new ArrayList();
        schemaProps1.add(new Property("testprop", "test"));
        schemaProps1.add(new Property("testprop3", "testAllowNO"));
        schemaProps1.add(new Property("testprop3", "testAllow"));
        FeatureSchema newSchema1 = new FeatureSchema(schemaProps1);
        testAddSchemaAllowableFields(openFeatureStore.getFeatureSchemaRepository(), newSchema, newSchema1);
    }

    @Test
    public void testAddViewSchemaAllowableFields() throws Exception {
        // create good schema
        List<Property> schemaProps = new ArrayList();
        schemaProps.add(new Property("testprop", "test"));
        schemaProps.add(new Property("testprop3", "testAllow"));
        ViewSchema newSchema = new ViewSchema(schemaProps);
        // create bad schema
        List<Property> schemaProps1 = new ArrayList();
        schemaProps1.add(new Property("testprop", "test"));
        schemaProps1.add(new Property("testprop3", "testAllowNO"));
        schemaProps1.add(new Property("testprop3", "testAllow"));
        ViewSchema newSchema1 = new ViewSchema(schemaProps1);
        testAddSchemaAllowableFields(openFeatureStore.getViewSchemaRepository(), newSchema, newSchema1);
    }

    private void testAddSchemaAllowableFields(SchemaRepository schemaRepo, Schema goodSchema, Schema badSchema) throws Exception {
        // create feature props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey<>("testprop", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey<>("testprop3",
                String.class, Cardinality.SET, new HashSet<>(Arrays.asList("testAllow", "testAllow2")));
        for (PrimitivePropertyKey key : Arrays.asList(testProp, testProp2)) {
            repo.createProperty(key);
        }
        // create model schema
        Schema schema = createAndReadSchema(goodSchema, schemaRepo);
        // verify good
        verifySchemaEquals(goodSchema, schema);
        // attempt create Schema with invalid allow value
        assertThrows(GraphIntegrityException.class, () -> {
            schemaRepo.createSchema(badSchema);
        });
    }

    @Test
    public void testReadSchema() throws GraphIntegrityException, Exception {
        // create props
        PropertyRepository propRepo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        propRepo.createProperty(testProp);
        // build schema
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testprop", "test"));
        ModelSchema newSchema = new ModelSchema(properties);
        Schema out = createAndReadSchema(newSchema, openFeatureStore.getModelSchemaRepository());
        out = openFeatureStore.getModelSchemaRepository().readSchema(out.getId());
        // verify outputs
        verifySchemaEquals(newSchema, out);
    }
    
    @Test
    public void testReadSchemaInvalidObject() throws Exception {
        // setup
        Model newModelRet = createBasicModel(openFeatureStore);
        // read with select
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getModelSchemaRepository().readSchema(newModelRet.getId());
        });
    }

    @Test
    public void testAddSchemaHasInternalProp() throws GraphIntegrityException, Exception {
        // create props
        PropertyRepository propRepo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        propRepo.createProperty(testProp);
        openFeatureStore.getConfig().setAllowInternalFieldActions(true);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey(OFSConfiguration.INTERNAL_FIELD_PREFIX + "testprop2", Long.class, Cardinality.SINGLE);
        propRepo.createProperty(testProp2);
        openFeatureStore.getConfig().setAllowInternalFieldActions(false);
        // build schema
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testprop", "test"));
        properties.add(new Property("_testprop2", 2L));
        ModelSchema newSchema = new ModelSchema(properties);
        // verify outputs
        assertThrows(InternalKeywordException.class, () -> {
            openFeatureStore.getModelSchemaRepository().createSchema(newSchema);
        });
    }

    @Test
    public void testAddSModelSchemaRefPropInvalidDefaultValue() throws Exception {
        List<Property> properties = setupAddSchemaRefPropInvalidDefaultValue();
        ModelSchema newSchema = new ModelSchema(properties);
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getModelSchemaRepository().createSchema(newSchema);
        });
    }

    @Test
    public void testAddFeatureSchemaRefPropInvalidDefaultValue() throws Exception {
        List<Property> properties = setupAddSchemaRefPropInvalidDefaultValue();
        FeatureSchema newSchema = new FeatureSchema(properties);
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getFeatureSchemaRepository().createSchema(newSchema);
        });
    }

    @Test
    public void testAddViewSchemaRefPropInvalidDefaultValue() throws Exception {
        List<Property> properties = setupAddSchemaRefPropInvalidDefaultValue();
        ViewSchema newSchema = new ViewSchema(properties);
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getViewSchemaRepository().createSchema(newSchema);
        });
    }

    // tests that if you add a feature which isn't a subtype of the specified model, it errors out
    private List<Property> setupAddSchemaRefPropInvalidDefaultValue() throws Exception {
        // create setup models/features
        Model model = createBasicModel(openFeatureStore);
        Model model2 = createBasicModel(openFeatureStore, false);
        Feature feature = createBasicFeature(openFeatureStore, model);
        // create props
        PropertyRepository propRepo = openFeatureStore.getPropertyRepository();
        RefPropertyKey testProp = new RefPropertyKey("testrefprop", model2.getId(), Cardinality.SINGLE);
        propRepo.createProperty(testProp);
        // create schema props
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testrefprop", feature.getId()));
        return properties;
    }

    @Test
    public void testDeleteSchema() throws GraphIntegrityException, Exception {
        // create props
        PropertyRepository propRepo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        propRepo.createProperty(testProp);
        // build schema
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testprop", "test"));
        ModelSchemaRepository repo = openFeatureStore.getModelSchemaRepository();
        ModelSchema newSchema = new ModelSchema(properties);
        Schema newSchemaOut = createAndReadSchema(newSchema, repo);
        // verify exists
        assertTrue(repo.readSchema(newSchemaOut.getId()) != null);
        // verify not exists after delete
        repo.deleteSchema(newSchemaOut.getId());
        assertThrows(VertexNotFoundException.class, () -> {
            repo.readSchema(newSchemaOut.getId());
        });
    }

    @Test
    public void testAddSchemaWithNotExistsProp() throws GraphIntegrityException, Exception {
        ModelSchemaRepository repo = openFeatureStore.getModelSchemaRepository();
        // build schema
        ModelSchema newSchema = new ModelSchema(new HashSet(Arrays.asList("testprop")));
        assertThrows(GraphIntegrityException.class, () -> {
            repo.createSchema(newSchema);
        });
        // build props map
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testprop", "default"));
        // build schema
        ModelSchema newSchema2 = new ModelSchema(properties);
        assertThrows(GraphIntegrityException.class, () -> {
            repo.createSchema(newSchema2);
        });
    }

    @Test
    public void testPropertyLockDuringModelSchemaCreation() throws Exception {
        ResourceLock mockLock = mock(ResourceLock.class);
        OpenFeatureStore customOFS = getOpenFeatureStore(OFSConfigurationParams.build().setResourceLock(mockLock));
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testprop", "test"));
        properties.add(new Property("testprop1", "test2"));
        ModelSchema newSchema = new ModelSchema(properties);
        testPropertyLockDuringSchemaCreation(customOFS.getModelSchemaRepository(), mockLock, newSchema);
    }

    @Test
    public void testPropertyLockDurinFeatureSchemaCreation() throws Exception {
        ResourceLock mockLock = mock(ResourceLock.class);
        OpenFeatureStore customOFS = getOpenFeatureStore(OFSConfigurationParams.build().setResourceLock(mockLock));
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testprop", "test"));
        properties.add(new Property("testprop1", "test2"));
        FeatureSchema newSchema = new FeatureSchema(properties);
        testPropertyLockDuringSchemaCreation(customOFS.getFeatureSchemaRepository(), mockLock, newSchema);
    }

    @Test
    public void testPropertyLockDuringViewSchemaCreation() throws Exception {
        ResourceLock mockLock = mock(ResourceLock.class);
        OpenFeatureStore customOFS = getOpenFeatureStore(OFSConfigurationParams.build().setResourceLock(mockLock));
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testprop", "test"));
        properties.add(new Property("testprop1", "test2"));
        ViewSchema newSchema = new ViewSchema(properties);
        testPropertyLockDuringSchemaCreation(customOFS.getViewSchemaRepository(), mockLock, newSchema);
    }

    private void testPropertyLockDuringSchemaCreation(SchemaRepository repo, ResourceLock mockLock, Schema newSchema) throws Exception {
        // crete setup props
        PropertyRepository propRepo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        propRepo.createProperty(testProp);
        PrimitivePropertyKey testProp1 = new PrimitivePropertyKey("testprop1", String.class, Cardinality.SINGLE);
        propRepo.createProperty(testProp1);
        // setup arg captors
        ArgumentCaptor<Set<String>> lockAcquireCaptor = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<Set<String>> lockReleaseCaptor = ArgumentCaptor.forClass(Set.class);
        // exec
        repo.createSchema(newSchema);
        // verify
        verify(mockLock, times(1)).acquireLocks(lockAcquireCaptor.capture());
        verify(mockLock, times(1)).releaseLocks(lockReleaseCaptor.capture());
        Set<String> expectedLockResources = new HashSet<>();
        expectedLockResources.add(DependencyResource.fromProperty(OFSType.JPROPERTY, "testprop").getResource());
        expectedLockResources.add(DependencyResource.fromProperty(OFSType.JPROPERTY, "testprop1").getResource());
        assertEquals(expectedLockResources, lockAcquireCaptor.getValue());
        assertEquals(expectedLockResources, lockReleaseCaptor.getValue());
    }

    @Test
    public void testNodeLockDuringModelSchemaDelete() throws Exception {
        ResourceLock mockLock = mock(ResourceLock.class);
        OpenFeatureStore customOFS = getOpenFeatureStore(OFSConfigurationParams.build().setResourceLock(mockLock));
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testprop", "test"));
        properties.add(new Property("testprop1", "test2"));
        ModelSchema newSchema = new ModelSchema(properties);
        testSchemaLockDuringSchemaDeletion(customOFS.getModelSchemaRepository(), mockLock, newSchema, OFSType.MODEL_SCHEMA);
    }

    @Test
    public void testNodeLockDurinFeatureSchemaDelete() throws Exception {
        ResourceLock mockLock = mock(ResourceLock.class);
        OpenFeatureStore customOFS = getOpenFeatureStore(OFSConfigurationParams.build().setResourceLock(mockLock));
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testprop", "test"));
        properties.add(new Property("testprop1", "test2"));
        FeatureSchema newSchema = new FeatureSchema(properties);
        testSchemaLockDuringSchemaDeletion(customOFS.getFeatureSchemaRepository(), mockLock, newSchema, OFSType.FEATURE_SCHEMA);
    }

    @Test
    public void testNodeLockDuringViewSchemaDelete() throws Exception {
        ResourceLock mockLock = mock(ResourceLock.class);
        OpenFeatureStore customOFS = getOpenFeatureStore(OFSConfigurationParams.build().setResourceLock(mockLock));
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testprop", "test"));
        properties.add(new Property("testprop1", "test2"));
        ViewSchema newSchema = new ViewSchema(properties);
        testSchemaLockDuringSchemaDeletion(customOFS.getViewSchemaRepository(), mockLock, newSchema, OFSType.VIEW_SCHEMA);
    }

    private void testSchemaLockDuringSchemaDeletion(SchemaRepository repo, ResourceLock mockLock, Schema newSchema, OFSType type) throws Exception {
        // crete setup props
        PropertyRepository propRepo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprop", String.class, Cardinality.SINGLE);
        propRepo.createProperty(testProp);
        PrimitivePropertyKey testProp1 = new PrimitivePropertyKey("testprop1", String.class, Cardinality.SINGLE);
        propRepo.createProperty(testProp1);
        // setup schema for delete
        Schema outCreate = createAndReadSchema(newSchema, repo);
        // exec
        repo.deleteSchema(outCreate.getId(), type);
        // verify (called twice since create hits it first)
        ArgumentCaptor<Set<String>> lockAcquireCaptor = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<Set<String>> lockReleaseCaptor = ArgumentCaptor.forClass(Set.class);
        verify(mockLock, times(2)).acquireLocks(lockAcquireCaptor.capture());
        verify(mockLock, times(2)).releaseLocks(lockReleaseCaptor.capture());
        Set<String> expectedLockResources = new HashSet<>();
        expectedLockResources.add(DependencyResource.fromNodeId(type, outCreate.getId()).getResource());
        assertEquals(expectedLockResources, lockAcquireCaptor.getAllValues().get(1));
        assertEquals(expectedLockResources, lockReleaseCaptor.getAllValues().get(1));
    }

    public static void verifySchemaEquals(Schema expected, Schema given) {
        // verify properties
        assertEquals(expected.getPropertyKeys(), given.getPropertyKeys());
        assertEquals(expected.getDefaultProperties().size(), given.getDefaultProperties().size());
        Map<String, List<Object>> expectedNameToDefaults = buildPropertyMap(expected);
        Map<String, List<Object>> givenNameToDefaults = buildPropertyMap(given);
        for (String propKey : expectedNameToDefaults.keySet()) {
            assertTrue(givenNameToDefaults.containsKey(propKey));
            List<Object> expectedDefaults = expectedNameToDefaults.get(propKey);
            List<Object> givenDefaults = givenNameToDefaults.get(propKey);
            boolean defaultsEqual = expectedDefaults.size() == givenDefaults.size()
                    && expectedDefaults.containsAll(givenDefaults)
                    && givenDefaults.containsAll(expectedDefaults);
            assertTrue(defaultsEqual, "Expected Property Key " + propKey + " defaults are not equal to given property values.");
        }
    }

    private static Map<String, List<Object>> buildPropertyMap(Schema schema) {
        Map<String, List<Object>> nameToDefaultValues = new HashMap<>();
        schema.getDefaultProperties().stream().forEach(p -> {
            List<Object> defaults = nameToDefaultValues.getOrDefault(p.getName(), new ArrayList());
            defaults.add(p.getValue());
            nameToDefaultValues.put(p.getName(), defaults);
        });
        return nameToDefaultValues;
    }

    // used to get around protected access in tests
    public static <E extends Schema> E createSchema(SchemaRepository<E> repo, E newSchema) throws Exception {
        return createAndReadSchema(newSchema, repo);
    }

    // used to get around protected access in tests
    public static void deleteSchema(SchemaRepository repo, String id, OFSType type) throws Exception {
        repo.deleteSchema(id, type);
    }
    
    public static <E extends Schema> E createAndReadSchema(E schema, SchemaRepository<E> repo) throws Exception {
        String id = repo.createSchema(schema);
        return repo.readSchema(id);
    }

}
