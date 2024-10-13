/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.index;

import java.io.IOException;
import org.lwing.ofs.core.OpenFeatureStore;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.exception.InternalKeywordException;
import org.lwing.ofs.core.api.index.Index;
import org.lwing.ofs.core.api.index.IndexElementType;
import org.lwing.ofs.core.api.index.IndexType;
import org.lwing.ofs.core.api.property.Cardinality;
import org.lwing.ofs.core.api.property.PrimitivePropertyKey;
import org.lwing.ofs.core.impl.GraphTest;
import org.lwing.ofs.core.impl.property.PropertyRepository;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.janusgraph.core.schema.Parameter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.lwing.ofs.core.api.config.OFSConfigurationParams;
import org.lwing.ofs.core.api.lock.ResourceLock;
import org.lwing.ofs.core.api.state.DependencyResource;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;
import org.mockito.ArgumentCaptor;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 *
 * @author Lucas Wing
 */
public class IndexTest extends GraphTest {

    @Test
    public void testBuildIndexVertex() throws GraphIntegrityException, InterruptedException, ExecutionException, Exception {
        IndexRepository indexRepo = openFeatureStore.getIndexRepository();
        PrimitivePropertyKey testProp = addTestProp(openFeatureStore, "testprop2", String.class);
        Map<String, Parameter[]> indexProps = buildIndexMap(testProp.getName());
        Index index = new Index("testIndex", indexProps, IndexElementType.VERTEX, IndexType.COMPOSITE, false, null);
        Index outIndex = indexRepo.createIndex(index, false);
        verifyIndexFieldsMatch(index, outIndex);
    }

    @Test
    public void testBuildInternalIndex() throws GraphIntegrityException, InterruptedException, ExecutionException, Exception {
        IndexRepository indexRepo = openFeatureStore.getIndexRepository();
        PrimitivePropertyKey testProp = addTestProp(openFeatureStore, "testprop2", String.class);
        Map<String, Parameter[]> indexProps = buildIndexMap(testProp.getName());
        Index index = new Index("_testIndex", indexProps, IndexElementType.VERTEX, IndexType.COMPOSITE, false, null);
        assertThrows(InternalKeywordException.class, () -> {
            indexRepo.createIndex(index, false);
        });
        openFeatureStore.getConfig().setAllowInternalFieldActions(true);
        indexRepo.createIndex(index, false);
    }

    @Test
    public void testBuildIndexEdge() throws GraphIntegrityException, InterruptedException, ExecutionException, Exception {
        IndexRepository indexRepo = openFeatureStore.getIndexRepository();
        PrimitivePropertyKey testProp = addTestProp(openFeatureStore, "testprop", String.class);
        Map<String, Parameter[]> indexProps = buildIndexMap(testProp.getName());
        Index index = new Index("testIndex", indexProps, IndexElementType.EDGE, IndexType.COMPOSITE, false, null);
        Index outIndex = indexRepo.createIndex(index, false);
        verifyIndexFieldsMatch(index, outIndex);
    }

    @Test
    public void testBuildUniqueIndex() throws GraphIntegrityException, InterruptedException, ExecutionException, Exception {
        IndexRepository indexRepo = openFeatureStore.getIndexRepository();
        PrimitivePropertyKey testProp = addTestProp(openFeatureStore, "testprop", String.class);
        Map<String, Parameter[]> indexProps = buildIndexMap(testProp.getName());
        Index index = new Index("testIndex", indexProps, IndexElementType.VERTEX, IndexType.COMPOSITE, true, null);
        Index outIndex = indexRepo.createIndex(index, false);
        verifyIndexFieldsMatch(index, outIndex);
    }

    @Test
    public void testBuildMultiPropIndex() throws GraphIntegrityException, InterruptedException, ExecutionException, Exception {
        IndexRepository indexRepo = openFeatureStore.getIndexRepository();
        PrimitivePropertyKey testProp = addTestProp(openFeatureStore, "testprop", String.class);
        PrimitivePropertyKey testProp2 = addTestProp(openFeatureStore, "testprop2", String.class);
        Map<String, Parameter[]> indexProps = buildIndexMap(testProp.getName(), testProp2.getName());
        Index index = new Index("testIndex", indexProps, IndexElementType.VERTEX, IndexType.COMPOSITE, true, null);
        Index outIndex = indexRepo.createIndex(index, false);
        verifyIndexFieldsMatch(index, outIndex);
    }

    @Test
    public void testGetIndicies() throws GraphIntegrityException, InterruptedException, ExecutionException, Exception {;
        IndexRepository indexRepo = openFeatureStore.getIndexRepository();

        PrimitivePropertyKey testProp = addTestProp(openFeatureStore, "testprop", String.class);
        Map<String, Parameter[]> indexProps = buildIndexMap(testProp.getName());
        Index index = new Index("testIndex", indexProps, IndexElementType.VERTEX, IndexType.COMPOSITE, true, null);
        Index outIndex1 = indexRepo.createIndex(index, false);

        testProp = addTestProp(openFeatureStore, "testprop2", String.class);
        indexProps = buildIndexMap(testProp.getName());
        index = new Index("testIndex2", indexProps, IndexElementType.VERTEX, IndexType.COMPOSITE, true, null);
        Index outIndex2 = indexRepo.createIndex(index, false);

        Set<Index> indicies = indexRepo.listIndices();
        assertTrue(verifyIndexInCollection(outIndex1.getName(), indicies));
        assertTrue(verifyIndexInCollection(outIndex2.getName(), indicies));
    }

    private boolean verifyIndexInCollection(String indexName, Collection<Index> coll) {
        for (Index i : coll) {
            if (i.getName().equals(indexName)) {
                return true;
            }
        }
        return false;
    }
    
    @Test
    public void testBuildMixedIndex() throws InterruptedException, GraphIntegrityException, ExecutionException, InternalKeywordException, IOException, Exception {
        // cleanup graph so I can remake with mixed index
        teardownTests();
        setupTests(true);
        // do actual test
        PrimitivePropertyKey testProp1 = new PrimitivePropertyKey("testExportProp1", String.class, Cardinality.SINGLE);
        openFeatureStore.getPropertyRepository().createProperty(testProp1);
        Index index1 = new Index("testIndex", buildIndexMap("testExportProp1"), IndexElementType.VERTEX, IndexType.MIXED, false, null);
        Index out = openFeatureStore.getIndexRepository().createIndex(index1, false);
        verifyIndexFieldsMatch(index1, out);
    }

    @Test
    public void testContainsIndex() throws GraphIntegrityException, InterruptedException, ExecutionException, Exception {
        IndexRepository indexRepo = openFeatureStore.getIndexRepository();
        PrimitivePropertyKey testProp = addTestProp(openFeatureStore, "testprop2", String.class);
        Map<String, Parameter[]> indexProps = buildIndexMap(testProp.getName());
        Index index = new Index("testIndex", indexProps, IndexElementType.VERTEX, IndexType.COMPOSITE, false, null);
        Index outIndex = indexRepo.createIndex(index, false);
        assertTrue(indexRepo.containsIndex(outIndex.getName()));
    }

    @Test
    public void testDoesNotContainIndex() throws GraphIntegrityException, InterruptedException, ExecutionException, Exception {
        IndexRepository indexRepo = openFeatureStore.getIndexRepository();
        assertFalse(indexRepo.containsIndex("testIndex"));
    }

    @Test
    public void testDeleteIndex() throws GraphIntegrityException, InterruptedException, ExecutionException, Exception {
        IndexRepository indexRepo = openFeatureStore.getIndexRepository();
        PrimitivePropertyKey testProp = addTestProp(openFeatureStore, "testprop", String.class);
        Map<String, Parameter[]> indexProps = buildIndexMap(testProp.getName());
        Index index = new Index("testIndex", indexProps, IndexElementType.VERTEX, IndexType.COMPOSITE, false, null);
        Index outIndex = indexRepo.createIndex(index, true);
        indexRepo.removeIndex(outIndex.getName());
        assertThrows(NullPointerException.class, () -> {
            indexRepo.readIndex(outIndex.getName());
        });
    }

    @Test
    public void testDeleteInternalIndex() throws GraphIntegrityException, InterruptedException, ExecutionException, Exception {
        IndexRepository indexRepo = openFeatureStore.getIndexRepository();
        PrimitivePropertyKey testProp = addTestProp(openFeatureStore, "testprop", String.class);
        Map<String, Parameter[]> indexProps = buildIndexMap(testProp.getName());
        Index index = new Index("_testIndex", indexProps, IndexElementType.VERTEX, IndexType.COMPOSITE, false, null);
        openFeatureStore.getConfig().setAllowInternalFieldActions(true);
        Index outIndex = indexRepo.createIndex(index, true);
        openFeatureStore.getConfig().setAllowInternalFieldActions(false);
        assertThrows(InternalKeywordException.class, () -> {
            indexRepo.removeIndex(outIndex.getName());
        });
        openFeatureStore.getConfig().setAllowInternalFieldActions(true);
        indexRepo.removeIndex(outIndex.getName());
        assertThrows(NullPointerException.class, () -> {
            indexRepo.readIndex(outIndex.getName());
        });
    }

    @Test
    public void testIndexLockDuringCreation() throws Exception {
        PrimitivePropertyKey testProp = addTestProp(openFeatureStore, "testprop", String.class);
        PrimitivePropertyKey testProp2 = addTestProp(openFeatureStore, "testprop2", String.class);
        // create custom ofs with mocks for locks
        ArgumentCaptor<Set<String>> lockAcquireCaptor = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<Set<String>> lockReleaseCaptor = ArgumentCaptor.forClass(Set.class);
        ResourceLock mockLock = mock(ResourceLock.class);
        OpenFeatureStore customOFS = getOpenFeatureStore(OFSConfigurationParams.build().setResourceLock(mockLock));
        // exec
        Map<String, Parameter[]> indexProps = buildIndexMap(testProp.getName(), testProp2.getName());
        Index index = new Index("testIndex", indexProps, IndexElementType.VERTEX, IndexType.COMPOSITE, false, null);
        Index outIndex = customOFS.getIndexRepository().createIndex(index, true);
        // verify
        verify(mockLock, times(1)).acquireLocks(lockAcquireCaptor.capture());
        verify(mockLock, times(1)).releaseLocks(lockReleaseCaptor.capture());
        Set<String> expectedLockResources = new HashSet<>();
        expectedLockResources.addAll(DependencyResource.getStringResources(DependencyResource.getPropertyResources(OFSType.JPROPERTY, indexProps.keySet())));
        assertEquals(expectedLockResources, lockAcquireCaptor.getValue());
        assertEquals(expectedLockResources, lockReleaseCaptor.getValue());
    }

    public static void verifyIndexFieldsMatch(Index expected, Index given) {
        assertEquals(expected.getName(), given.getName());
        assertEquals(expected.getFieldType(), given.getFieldType());
        assertEquals(expected.getIndexType(), given.getIndexType());
        assertEquals(expected.isUnique(), given.isUnique());
        assertEquals(expected.getProperties().keySet(), given.getProperties().keySet());
        for (Entry<String, Parameter[]> prop : expected.getProperties().entrySet()) {
            if (prop.getValue() != null) {
                Parameter[] givenParams = given.getProperties().get(prop.getKey());
                // only check first val since that's all that matters with mapping
                assertEquals(prop.getValue()[0].key(), givenParams[0].key());
                assertEquals(prop.getValue()[0].value().toString(), givenParams[0].value().toString());
            } else {
                assertEquals(prop.getValue(), given.getProperties().get(prop.getKey()));
            }
        }
    }

    private PrimitivePropertyKey addTestProp(OpenFeatureStore ofs, String propName, Class propType) throws GraphIntegrityException, Exception {
        PropertyRepository propRepo = ofs.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey(propName, propType, Cardinality.SINGLE);
        propRepo.createProperty(testProp);
        return propRepo.readPrimitiveProperty(propName).get();
    }

    public static Map<String, Parameter[]> buildIndexMap(String... indexKey) {
        Map<String, Parameter[]> paramMap = new HashMap<>();
        for (String index : indexKey) {
            paramMap.put(index, null);
        }
        return paramMap;
    }

}
