/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.view;

import org.lwing.ofs.core.api.view.TreeView;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.config.OFSConfiguration;
import static org.lwing.ofs.core.api.config.OFSConfiguration.INTERNAL_FIELD_PREFIX;
import org.lwing.ofs.core.api.VertexType;
import org.lwing.ofs.core.api.exception.VertexNotFoundException;
import org.lwing.ofs.core.api.property.Property;
import org.lwing.ofs.core.api.schema.Schema;
import org.lwing.ofs.core.api.view.View;
import org.lwing.ofs.core.api.view.ViewLevel;
import org.lwing.ofs.core.impl.PropertyUtil;
import org.lwing.ofs.core.impl.SchemaVertexRepository;
import org.lwing.ofs.core.impl.lock.CloseableResourceLock;
import org.lwing.ofs.core.impl.property.PropertyRepository;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.V;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;
import org.lwing.ofs.core.impl.model.ModelRepository;

/**
 *
 * @author Lucas Wing
 */
public class ViewRepository extends SchemaVertexRepository<View> {

    // pointer at model schema on models
    public static final String VIEW_SCHEMA = INTERNAL_FIELD_PREFIX + "view_schema";

    // view looking at a model
    public static final String VIEWS_MODEL_LABEL = INTERNAL_FIELD_PREFIX + "views_model";

    // view looking at a view
    public static final String VIEWS_VIEW_LABEL = INTERNAL_FIELD_PREFIX + "views_view";

    private final ViewSchemaRepository viewSchemaRepository;
    
    private final ModelRepository modelRepository;

    public ViewRepository(
            JanusGraph graph, 
            OFSConfiguration config, 
            PropertyRepository propertyRepository, 
            ViewSchemaRepository viewSchemaRepository,
            ModelRepository mdelRepository
    ) {
        super(VertexType.VIEW, graph, propertyRepository, config);
        this.viewSchemaRepository = viewSchemaRepository;
        this.modelRepository = mdelRepository;
    }

    /**
     * Creates a view object in the configured graph. Views are used to organize
     * models and other views in custom viewable structures.
     *
     * @param view object to create
     * @return created view's id
     * @throws GraphIntegrityException if the view being created does not match
     * the schema
     * @throws Exception for other JanusGraph related errors
     */
    public String createView(View view) throws GraphIntegrityException, Exception {
        try ( GraphTraversalSource g = getTraversalSource()) {
            try ( Transaction tx = g.tx()) {
                String viewId = (String) addVertex(g, view.getId()).next().id();
                try ( CloseableResourceLock lock = acquireLock(view)) {
                    // get view schema
                    Schema viewSchema = viewSchemaRepository.readSchema(view.getViewSchemaId(), g);
                    // calc props based off schema
                    List<Property> inpProps = calcPropsToUse(viewSchema, view.getProperties());
                    // add attr properties
                    addPropsToVertex(inpProps, buildPropInfoMap(viewSchema), viewId, g);
                    // link view schema
                    g.V(viewId).addE(VIEW_SCHEMA).to(V(viewSchema.getId())).next();
                    // add links to models
                    for (String modelId: view.getModelIds()) {
                        // load model and verify it exists as a model
                        modelRepository.readModel(modelId);
                        // add link to model
                        g.V(viewId).addE(ViewRepository.VIEWS_MODEL_LABEL).to(V(modelId)).next();
                    }
                    // add links to views
                    for (String linkedViewId: view.getViewIds()) {
                        // load view and verify it exists as a model
                        readView(linkedViewId, g, PropertyUtil.EMPTY_SELECT);
                        // add link to view
                        g.V(viewId).addE(ViewRepository.VIEWS_VIEW_LABEL).to(V(linkedViewId)).next();
                    }
                    tx.commit();
                }
                return viewId;
            }
        }
    }

    /**
     * Reads a view object using the input id
     *
     * @param id unique view node's identifier
     * @param select fields to return on the view
     * @return view object
     * @throws Exception for other JanusGraph related errors
     */
    public View readView(String id, Set<String> select) throws Exception {
        return readView(id, Optional.of(select));
    }

    /**
     * Reads a view object using the input id Returns all fields on the view
     *
     * @param id unique view node's identifier
     * @return view object
     * @throws Exception for other JanusGraph related errors
     */
    public View readView(String id) throws Exception {
        return readView(id, PropertyUtil.ALL_SELECT);
    }

    /**
     * Reads a view object using the input id
     *
     * @param id unique view node's identifier
     * @param select fields to return on the view (empty optional means all
     * fields)
     * @return view object
     * @throws Exception for other JanusGraph related errors
     */
    public View readView(String id, Optional<Set<String>> select) throws Exception {
        try ( GraphTraversalSource g = getTraversalSource()) {
            return readView(id, g, select);
        }
    }

    protected View readView(String id, GraphTraversalSource g, Optional<Set<String>> select) throws VertexNotFoundException, Exception {
        return new RepoView(readVertexFromGraph(id, g), select, g);
    }

    /**
     * Deletes the stated view object from the graph if it's not being
     * referenced
     *
     * @param viewId
     * @throws GraphIntegrityException if the view is being referenced
     * @throws Exception for other JanusGraph related errors
     */
    public void deleteView(String viewId) throws GraphIntegrityException, Exception {
        deleteVertex(viewId, OFSType.VIEW);
    }

    /**
     * Cost-Intensive way of loading every view and model in the tree under a
     * view.This should not typically be used, look at using
     * {@link #getViewLevel} instead.
     *
     * @param viewId
     * @param viewSelect optional of a set of properties to return on the views.
     * Empty optional means all fields.
     * @param modelSelect optional of a set of properties to return on the
     * models. Empty optional means all fields.
     * @return
     * @throws Exception
     */
    public TreeView buildTreeView(String viewId, Optional<Set<String>> viewSelect, Optional<Set<String>> modelSelect) throws Exception {
        try ( GraphTraversalSource g = getTraversalSource()) {
            return new TreeView(g.V(viewId).next(), g, modelSelect, viewSelect);
        }
    }

    /**
     * Cost-Intensive way of loading every view and model in the tree under a
     * view.This should not typically be used, look at using
     * {@link #getViewLevel} instead.
     *
     * @param viewId
     * @return
     * @throws Exception
     */
    public TreeView buildTreeView(String viewId) throws Exception {
        return buildTreeView(viewId, PropertyUtil.ALL_SELECT, PropertyUtil.ALL_SELECT);
    }

    /**
     * Lists off all the models and views under the input view's id. Subsequent
     * calls can be mode to load the ViewLevels for all views returned to
     * eventually end up with the full view tree.
     *
     * @param viewId
     * @param viewSelect optional of a set of properties to return on the views.
     * Empty optional means all fields.
     * @param modelSelect optional of a set of properties to return on the
     * models. Empty optional means all fields.
     * @return object containing all models and views under the input view's id
     * @throws Exception
     */
    public ViewLevel getViewLevel(String viewId, Optional<Set<String>> viewSelect, Optional<Set<String>> modelSelect) throws Exception {
        try ( GraphTraversalSource g = getTraversalSource()) {
            return new RepoViewLevel(readView(viewId, g, PropertyUtil.EMPTY_SELECT), g, viewSelect, modelSelect);
        }
    }

    /**
     * Lists off all the models and views under the input view's id. Subsequent
     * calls can be mode to load the ViewLevels for all views returned to
     * eventually end up with the full view tree.
     *
     * @param viewId
     * @return object containing all models and views under the input view's id
     * @throws Exception
     */
    public ViewLevel getViewLevel(String viewId) throws Exception {
        return getViewLevel(viewId, PropertyUtil.ALL_SELECT, PropertyUtil.ALL_SELECT);
    }

    @Override
    protected final View buildType(Vertex v, GraphTraversalSource g, Optional<Set<String>> select) {
        return new RepoView(v, select, g);
    }

}
