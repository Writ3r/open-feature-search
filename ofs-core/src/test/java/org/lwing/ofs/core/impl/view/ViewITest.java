/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.view;

import org.lwing.ofs.core.OpenFeatureStore;
import org.lwing.ofs.core.api.config.OFSConfigurationParams;
import org.lwing.ofs.core.api.view.TreeView;
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
import org.lwing.ofs.core.api.view.View;
import org.lwing.ofs.core.api.view.ViewLevel;
import org.lwing.ofs.core.impl.GraphTest;
import org.lwing.ofs.core.impl.model.ModelTest;
import org.lwing.ofs.core.impl.property.PropertyRepository;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
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
public class ViewITest extends GraphTest {

    @Test
    public void testAddView() throws Exception {
        // create models to ref
        Model newModelRet1 = createBasicModel(openFeatureStore);
        Model newModelRet2 = createBasicModel(openFeatureStore, false);
        // create views to ref
        View view = new View(new HashSet<>(), new HashSet<>(), new ArrayList<>(), createViewSchema(new ArrayList<>()).getId());
        View outView = createAndReadView(view);
        View view2 = new View(new HashSet<>(), new HashSet<>(), new ArrayList<>(), createViewSchema(new ArrayList<>()).getId());
        View outView2 = createAndReadView(view2);
        // create actual view
        View viewActual = new View(
                new HashSet<>(Arrays.asList(newModelRet1.getId(), newModelRet2.getId())),
                new HashSet<>(Arrays.asList(outView.getId(), outView2.getId())),
                new ArrayList<>(),
                createViewSchema(new ArrayList<>()).getId()
        );
        View outViewActual = createAndReadView(viewActual);
        // verify actions
        verifyViewEquals(viewActual, outViewActual);
    }
    
    @Test
    public void testAddViewWithId() throws Exception {
        // create actual view
        View viewActual = new View(
                "testId",
                new HashSet<>(),
                new HashSet<>(),
                new ArrayList<>(),
                createViewSchema(new ArrayList<>()).getId()
        );
        View outViewActual = createAndReadView(viewActual);
        // verify id
        assertEquals("testId", outViewActual.getId());
    }

    @Test
    public void testAddViewWithProps() throws Exception {
        // create default model & feature
        Model newModelRet = createBasicModel(openFeatureStore);
        Feature newFeature = createBasicFeature(openFeatureStore, newModelRet);
        Feature newFeature2 = createBasicFeature(openFeatureStore, newModelRet);
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testViewpropNormal", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp4 = new PrimitivePropertyKey("testViewpropNormal2", String.class, Cardinality.SINGLE);
        RefPropertyKey testProp2 = new RefPropertyKey("testViewpropRef", newModelRet.getId(), Cardinality.SINGLE);
        RefPropertyKey testProp3 = new RefPropertyKey("testViewpropRef3", newModelRet.getId(), Cardinality.SINGLE);
        repo.createProperty(testProp);
        repo.createProperty(testProp2);
        repo.createProperty(testProp3);
        repo.createProperty(testProp4);
        // create schema
        List<Property> properties = new ArrayList<>();
        properties.add(new Property("testViewpropNormal"));
        properties.add(new Property("testViewpropRef", newFeature.getId()));
        properties.add(new Property("testViewpropRef3"));
        properties.add(new Property("testViewpropNormal2", "testDefault"));
        // make view schema
        Schema viewSchema = createViewSchema(properties);
        // create view
        List<Property> viewProps = new ArrayList<>();
        viewProps.add(new Property("testViewpropRef3", newFeature2.getId()));
        viewProps.add(new Property("testViewpropNormal", "testProp"));
        viewProps.add(new Property("testViewpropNormal2", "testOverwrite"));
        viewProps.add(new Property("propNotInSchema", "aaaaa"));
        View viewActual = new View(new HashSet<>(), new HashSet<>(), viewProps, viewSchema.getId());
        View outViewActual = createAndReadView(viewActual);
        // verify actions
        viewActual.getProperties().remove(3); // removes the prop for propNotInSchema
        viewActual.getProperties().add(new Property("testViewpropRef", newFeature.getId()));
        verifyViewEquals(viewActual, outViewActual);
    }
    
    @Test
    public void testAddViewInvalidView() throws Exception {
        // create model to ref
        Model newModelRet1 = createBasicModel(openFeatureStore);
        // create actual view
        View viewActual = new View(
                new HashSet<>(Arrays.asList()),
                new HashSet<>(Arrays.asList(newModelRet1.getId())),
                new ArrayList<>(),
                createViewSchema(new ArrayList<>()).getId()
        );
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getViewRepository().createView(viewActual);
        });
    }
    
    @Test
    public void testAddViewInvalidModel() throws Exception {
        // create view to ref
        View view = new View(new HashSet<>(), new HashSet<>(), new ArrayList<>(), createViewSchema(new ArrayList<>()).getId());
        View outView = createAndReadView(view);
        // create actual view
        View viewActual = new View(
                new HashSet<>(Arrays.asList(outView.getId())),
                new HashSet<>(Arrays.asList()),
                new ArrayList<>(),
                createViewSchema(new ArrayList<>()).getId()
        );
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getViewRepository().createView(viewActual);
        });
    }
    
    @Test
    public void testReadViewInvalidObject() throws Exception {
        // setup
        Model newModelRet = createBasicModel(openFeatureStore);
        // read with select
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getViewRepository().readView(newModelRet.getId());
        });
    }

    @Test
    public void testReadViewWithSelect() throws Exception {
        // setup
        Model newModelRet = createBasicModel(openFeatureStore);
        Feature newFeature = createBasicFeature(openFeatureStore, newModelRet);
        View outViewActual = setupSelectTest(newModelRet, newFeature);
        // read with select
        View readView = openFeatureStore.getViewRepository().readView(outViewActual.getId(),
                new HashSet<>(Arrays.asList("testrefprop3", "testprimprop2")));
        // verify output
        verifyViewSelect(outViewActual, readView, newFeature);
    }

    @Test
    public void testSearchViewsWithSelect() throws Exception {
        // setup
        Model newModelRet = createBasicModel(openFeatureStore);
        Feature newFeature = createBasicFeature(openFeatureStore, newModelRet);
        View outViewActual = setupSelectTest(newModelRet, newFeature);
        // read with search
        Set<String> selectSet = new HashSet<>(Arrays.asList("testrefprop3", "testprimprop2"));
        View readView = openFeatureStore.getViewRepository().search(
                (g) -> {
                    return g.V(outViewActual.getId());
                }, Optional.of(selectSet)).get(0);
        // verify output
        verifyViewSelect(outViewActual, readView, newFeature);
    }

    private void verifyViewSelect(View outViewActual, View verifyView, Feature newFeature) {
        outViewActual.getProperties().clear();
        outViewActual.getProperties().add(new Property("testrefprop3", newFeature.getId()));
        outViewActual.getProperties().add(new Property("testprimprop2", "aaa"));
        verifyViewEquals(outViewActual, verifyView);
    }

    private View setupSelectTest(Model inpModel, Feature newFeature) throws Exception {
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey("testprimprop", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testprimprop2", String.class, Cardinality.LIST);
        RefPropertyKey testRefProp = new RefPropertyKey("testrefprop", inpModel.getId(), Cardinality.SINGLE);
        RefPropertyKey testRefProp3 = new RefPropertyKey("testrefprop3", inpModel.getId(), Cardinality.SINGLE);
        repo.createProperty(testProp);
        repo.createProperty(testProp2);
        repo.createProperty(testRefProp);
        repo.createProperty(testRefProp3);
        // create view schema
        List<Property> fproperties = new ArrayList<>();
        fproperties.add(new Property("testprimprop"));
        fproperties.add(new Property("testprimprop2"));
        fproperties.add(new Property("testrefprop"));
        fproperties.add(new Property("testrefprop3"));
        // make view schema
        Schema viewSchemaOut = createViewSchema(fproperties);
        // make view
        List<Property> viewProps = new ArrayList<>();
        viewProps.add(new Property("testrefprop", newFeature.getId()));
        viewProps.add(new Property("testrefprop3", newFeature.getId()));
        viewProps.add(new Property("testprimprop", "testProp"));
        viewProps.add(new Property("testprimprop2", "aaa"));
        View viewActual = new View(new HashSet<>(), new HashSet<>(), viewProps, viewSchemaOut.getId());
        return createAndReadView(viewActual);
    }

    @Test
    public void testAddViewAllowableFields() throws Exception {
        // create props
        PropertyRepository repo = openFeatureStore.getPropertyRepository();
        PrimitivePropertyKey testProp = new PrimitivePropertyKey<>("testprop", String.class, Cardinality.SINGLE);
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey<>("testprop3",
                String.class, Cardinality.SET, new HashSet<>(Arrays.asList("testAllow", "testAllow2")));
        for (PrimitivePropertyKey key : Arrays.asList(testProp, testProp2)) {
            repo.createProperty(key);
        }
        // create view schema
        ViewSchema newSchema = new ViewSchema(new HashSet<>(Arrays.asList("testprop", "testprop3")));
        ViewSchema viewSchema = createAndReadViewSchema(newSchema);
        // create view
        List<Property> viewProps = new ArrayList<>();
        viewProps.add(new Property("testprop", "test"));
        viewProps.add(new Property("testprop3", "testAllow"));
        View viewActual = new View(
                new HashSet<>(),
                new HashSet<>(),
                viewProps,
                viewSchema.getId()
        );
        View outViewActual = createAndReadView(viewActual);
        // verify good
        verifyViewEquals(viewActual, outViewActual);
        // attempt create view with invalid allow value
        viewProps = new ArrayList<>();
        viewProps.add(new Property("testprop", "test"));
        viewProps.add(new Property("testprop3", "testAllowNO"));
        viewProps.add(new Property("testprop3", "testAllow"));
        View viewActual2 = viewActual = new View(
                new HashSet<>(),
                new HashSet<>(),
                viewProps,
                viewSchema.getId()
        );
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getViewRepository().createView(viewActual2);
        });
    }

    @Test
    public void testAddViewNotEnoughProps() throws Exception {
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testprop2", Long.class, Cardinality.SINGLE);
        openFeatureStore.getPropertyRepository().createProperty(testProp2);
        // create view
        Schema viewSchema = createViewSchema(Arrays.asList(new Property("testprop2")));
        View view = new View(new HashSet<>(), new HashSet<>(), Arrays.asList(), viewSchema.getId());
        // verify actions
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getViewRepository().createView(view);
        });
    }

    @Test
    public void testReadView() throws Exception {
        // create models to ref
        Model newModelRet1 = createBasicModel(openFeatureStore);
        Model newModelRet2 = createBasicModel(openFeatureStore, false);
        // create view
        View viewActual = new View(
                new HashSet<>(Arrays.asList(newModelRet1.getId(), newModelRet2.getId())),
                new HashSet<>(),
                new ArrayList<>(),
                createViewSchema(new ArrayList<>()).getId()
        );
        View outViewActual = createAndReadView(viewActual);
        // verify actions
        verifyViewEquals(viewActual, openFeatureStore.getViewRepository().readView(outViewActual.getId()));
    }

    @Test
    public void testDeleteView() throws Exception {
        // create models to ref
        Model newModelRet1 = createBasicModel(openFeatureStore);
        // create view
        View viewActual = new View(
                new HashSet<>(Arrays.asList(newModelRet1.getId())),
                new HashSet<>(),
                new ArrayList<>(),
                createViewSchema(new ArrayList<>()).getId()
        );
        View outViewActual = createAndReadView(viewActual);
        // verify actions
        verifyViewEquals(viewActual, openFeatureStore.getViewRepository().readView(outViewActual.getId()));
        openFeatureStore.getViewRepository().deleteView(outViewActual.getId());
        assertThrows(VertexNotFoundException.class, () -> {
            openFeatureStore.getViewRepository().readView(outViewActual.getId());
        });
    }

    @Test
    public void testDeleteRefedView() throws Exception {
        // create models to ref
        Model newModelRet1 = createBasicModel(openFeatureStore);
        // create view 1
        View view1 = new View(
                new HashSet<>(Arrays.asList(newModelRet1.getId())),
                new HashSet<>(),
                new ArrayList<>(),
                createViewSchema(new ArrayList<>()).getId()
        );
        View outView1 = createAndReadView(view1);
        // create view actual
        View viewActual = new View(
                new HashSet<>(Arrays.asList(newModelRet1.getId())),
                new HashSet<>(Arrays.asList(outView1.getId())),
                new ArrayList<>(),
                createViewSchema(new ArrayList<>()).getId()
        );
        View outViewActual = createAndReadView(viewActual);
        // verify actions
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getViewRepository().deleteView(outView1.getId());
        });
        assertThrows(GraphIntegrityException.class, () -> {
            openFeatureStore.getModelRepository().deleteModel(newModelRet1.getId());
        });
        openFeatureStore.getViewRepository().deleteView(outViewActual.getId());
        openFeatureStore.getViewRepository().deleteView(outView1.getId());
        openFeatureStore.getModelRepository().deleteModel(newModelRet1.getId());
    }

    @Test
    public void testViewLevel() throws Exception {
        // create models to ref
        Model newModelRet1 = createBasicModel(openFeatureStore);
        Model newModelRet2 = createBasicModel(openFeatureStore, false);
        // create views to ref
        View view = new View(new HashSet<>(), new HashSet<>(), new ArrayList<>(), createViewSchema(new ArrayList<>()).getId());
        View outView = createAndReadView(view);
        View view2 = new View(new HashSet<>(), new HashSet<>(), new ArrayList<>(), createViewSchema(new ArrayList<>()).getId());
        View outView2 = createAndReadView(view2);
        // create actual view
        View viewActual = new View(
                new HashSet<>(Arrays.asList(newModelRet1.getId(), newModelRet2.getId())),
                new HashSet<>(Arrays.asList(outView.getId(), outView2.getId())),
                new ArrayList<>(),
                createViewSchema(new ArrayList<>()).getId()
        );
        View outViewActual = createAndReadView(viewActual);
        // verify actions
        ViewLevel viewLevel = openFeatureStore.getViewRepository().getViewLevel(outViewActual.getId());
        assertEquals(new HashSet<>(Arrays.asList(outView, outView2)), viewLevel.getViews());
        assertEquals(new HashSet<>(Arrays.asList(newModelRet1, newModelRet2)), viewLevel.getModels());
    }

    @Test
    public void testViewSearch() throws Exception {
        PrimitivePropertyKey testProp2 = new PrimitivePropertyKey("testprop2", Long.class, Cardinality.SINGLE);
        openFeatureStore.getPropertyRepository().createProperty(testProp2);
        // create models to ref
        Model newModelRet1 = createBasicModel(openFeatureStore);
        Model newModelRet2 = createBasicModel(openFeatureStore, false);
        // create views to ref
        Schema viewSchema = createViewSchema(Arrays.asList(new Property("testprop2")));
        View view = new View(new HashSet<>(), new HashSet<>(), Arrays.asList(new Property("testprop2", 1L)), viewSchema.getId());
        View outView = createAndReadView(view);
        View view2 = new View(new HashSet<>(), new HashSet<>(), Arrays.asList(new Property("testprop2", 2L)), viewSchema.getId());
        View outView2 = createAndReadView(view2);
        // create last view
        View viewActual = new View(
                new HashSet<>(Arrays.asList(newModelRet1.getId(), newModelRet2.getId())),
                new HashSet<>(Arrays.asList(outView.getId(), outView2.getId())),
                Arrays.asList(new Property("testprop2", 3L)),
                viewSchema.getId()
        );
        View outViewActual = createAndReadView(viewActual);
        // verify actions
        List<View> out = openFeatureStore.getViewRepository().search((g) -> {
            return g.V().order().by("testprop2");
        });
        assertEquals(3, out.size());
        assertEquals(outView, out.get(0));
        assertEquals(outView2, out.get(1));
        assertEquals(outViewActual, out.get(2));
    }

    @Test
    public void testViewModificationDuringSearch() throws Exception {
        // create model to ref
        Model newModelRet1 = createBasicModel(openFeatureStore);
        // create last view
        View viewActual = new View(
                new HashSet<>(Arrays.asList(newModelRet1.getId())),
                new HashSet<>(),
                new ArrayList<>(),
                createViewSchema(new ArrayList<>()).getId()
        );
        View outViewActual = createAndReadView(viewActual);
        // exec search
        assertThrows(VerificationException.class, () -> {
            openFeatureStore.getViewRepository().search((g) -> {
                try ( Transaction tx = g.tx()) {
                    g.V(outViewActual.getId()).property("testprop", "test").next();
                    tx.commit();
                }
                return g.V(outViewActual.getId());
            });
        });
    }

    @Test
    public void testViewTree() throws Exception {
        // create models to ref
        Model newModelRet1 = createBasicModel(openFeatureStore);
        Model newModelRet2 = createBasicModel(openFeatureStore, false);
        // create view to ref 1st level
        View view0 = new View(new HashSet<>(Arrays.asList(newModelRet1.getId())), new HashSet<>(), new ArrayList<>(), createViewSchema(new ArrayList<>()).getId());
        View outView0 = createAndReadView(view0);
        // create views to ref 
        View view = new View(new HashSet<>(), new HashSet<>(Arrays.asList(outView0.getId())), new ArrayList<>(), createViewSchema(new ArrayList<>()).getId());
        View outView = createAndReadView(view);
        View view2 = new View(new HashSet<>(), new HashSet<>(), new ArrayList<>(), createViewSchema(new ArrayList<>()).getId());
        openFeatureStore.getViewRepository().createView(view2);
        // create actual view
        View viewActual = new View(
                new HashSet<>(Arrays.asList(newModelRet1.getId(), newModelRet2.getId())),
                new HashSet<>(Arrays.asList(outView.getId())),
                new ArrayList<>(),
                createViewSchema(new ArrayList<>()).getId()
        );
        View outViewActual = createAndReadView(viewActual);
        TreeView viewTree = openFeatureStore.getViewRepository().buildTreeView(outViewActual.getId());
        // verify
        assertEquals(new HashSet<>(Arrays.asList(newModelRet1, newModelRet2)), viewTree.getChildModels());
        TreeView nextLevel = viewTree.getChildViews().iterator().next();
        assertEquals(outView.getId(), nextLevel.getId());
        TreeView lastLevel = nextLevel.getChildViews().iterator().next();
        assertEquals(outView0.getId(), lastLevel.getId());
        assertEquals(new HashSet<>(Arrays.asList(newModelRet1)), lastLevel.getChildModels());
    }

    @Test
    public void testLockDuringViewCreation() throws Exception {
        // create models to ref
        Model newModelRet1 = createBasicModel(openFeatureStore);
        Model newModelRet2 = createBasicModel(openFeatureStore, false);
        // create view to ref
        Schema viewSchema = createViewSchema(new ArrayList<>());
        View view0 = new View(new HashSet<>(Arrays.asList(newModelRet1.getId())), new HashSet<>(), new ArrayList<>(), viewSchema.getId());
        View outView0 = createAndReadView(view0);
        // create custom ofs with mocks for locks
        ArgumentCaptor<Set<String>> lockAcquireCaptor = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<Set<String>> lockReleaseCaptor = ArgumentCaptor.forClass(Set.class);
        ResourceLock mockLock = mock(ResourceLock.class);
        OpenFeatureStore customOFS = getOpenFeatureStore(OFSConfigurationParams.build().setResourceLock(mockLock));
        // exec
        View viewActual = new View(
                new HashSet<>(Arrays.asList(newModelRet1.getId(), newModelRet2.getId())),
                new HashSet<>(Arrays.asList(outView0.getId())),
                new ArrayList<>(),
                viewSchema.getId()
        );
        customOFS.getViewRepository().createView(viewActual);
        // verify
        verify(mockLock, times(1)).acquireLocks(lockAcquireCaptor.capture());
        verify(mockLock, times(1)).releaseLocks(lockReleaseCaptor.capture());
        Set<String> expectedLockResources = new HashSet<>();
        expectedLockResources.add(DependencyResource.fromNodeId(OFSType.VIEW, outView0.getId()).getResource());
        expectedLockResources.add(DependencyResource.fromNodeId(OFSType.MODEL, newModelRet1.getId()).getResource());
        expectedLockResources.add(DependencyResource.fromNodeId(OFSType.MODEL, newModelRet2.getId()).getResource());
        expectedLockResources.add(DependencyResource.fromNodeId(OFSType.VIEW_SCHEMA, viewSchema.getId()).getResource());
        assertEquals(expectedLockResources, lockAcquireCaptor.getValue());
        assertEquals(expectedLockResources, lockReleaseCaptor.getValue());
    }

    @Test
    public void testLockDuringViewDeletion() throws Exception {
        // create models to ref
        Model newModelRet1 = createBasicModel(openFeatureStore);
        // create view to ref
        Schema viewSchema = createViewSchema(new ArrayList<>());
        View view0 = new View(new HashSet<>(Arrays.asList(newModelRet1.getId())), new HashSet<>(), new ArrayList<>(), viewSchema.getId());
        View outView0 = createAndReadView(view0);
        // create custom ofs with mocks for locks
        ArgumentCaptor<Set<String>> lockAcquireCaptor = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<Set<String>> lockReleaseCaptor = ArgumentCaptor.forClass(Set.class);
        ResourceLock mockLock = mock(ResourceLock.class);
        OpenFeatureStore customOFS = getOpenFeatureStore(OFSConfigurationParams.build().setResourceLock(mockLock));
        // exec
        customOFS.getViewRepository().deleteView(outView0.getId());
        // verify
        verify(mockLock, times(1)).acquireLocks(lockAcquireCaptor.capture());
        verify(mockLock, times(1)).releaseLocks(lockReleaseCaptor.capture());
        Set<String> expectedLockResources = new HashSet<>();
        expectedLockResources.add(DependencyResource.fromNodeId(OFSType.VIEW, outView0.getId()).getResource());
        assertEquals(expectedLockResources, lockAcquireCaptor.getValue());
        assertEquals(expectedLockResources, lockReleaseCaptor.getValue());
    }

    // test view tree
    private ViewSchema createViewSchema(List<Property> properties) throws Exception {
        ViewSchema newSchema = new ViewSchema(properties);
        return createAndReadViewSchema(newSchema);
    }

    public static void verifyViewEquals(View expectedView, View givenView) {
        // verify schema
        assertEquals(expectedView.getViewSchemaId(), givenView.getViewSchemaId());
        // verify view ids
        assertEquals(expectedView.getViewIds(), givenView.getViewIds());
        // verify model ids
        assertEquals(expectedView.getModelIds().size(), givenView.getModelIds().size());
        // verify properties
        ModelTest.verifyPropertiesEqual(expectedView.getProperties(), givenView.getProperties());
    }
    
    private View createAndReadView(View input) throws Exception {
        String id = openFeatureStore.getViewRepository().createView(input);
        return openFeatureStore.getViewRepository().readView(id);
    }
    
    private ViewSchema createAndReadViewSchema(ViewSchema inp) throws Exception {
        String id = openFeatureStore.getViewSchemaRepository().createSchema(inp);
        return openFeatureStore.getViewSchemaRepository().readSchema(id);
    }

}
