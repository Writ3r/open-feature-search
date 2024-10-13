/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.state;

import java.nio.file.Path;
import org.lwing.ofs.core.api.config.OFSConfiguration;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.exception.InternalException;
import org.lwing.ofs.core.api.exception.InternalKeywordException;
import org.lwing.ofs.core.api.state.GraphStateResponse;
import org.lwing.ofs.core.impl.feature.FeatureRepository;
import org.lwing.ofs.core.impl.index.IndexRepository;
import org.lwing.ofs.core.impl.model.ModelRepository;
import org.lwing.ofs.core.impl.model.ModelSchemaRepository;
import org.lwing.ofs.core.impl.property.PropertyRepository;
import org.lwing.ofs.core.impl.state.manager.ExportStateManager;
import org.lwing.ofs.core.impl.state.manager.ImportStateManager;
import org.lwing.ofs.core.impl.state.storage.GraphStorage;
import org.lwing.ofs.core.impl.view.ViewRepository;
import org.lwing.ofs.core.impl.view.ViewSchemaRepository;

/**
 *
 * @author Lucas Wing
 */
public class StateManager {

    private final IndexRepository indexRepository;

    private final PropertyRepository propertyRepository;

    private final FeatureRepository featureRepository;

    private final ModelRepository modelRepository;

    private final ViewRepository viewRepository;

    private final ModelSchemaRepository modelSchemaRepository;

    private final ViewSchemaRepository viewSchemaRepository;
    
    private final OFSConfiguration config;

    public StateManager(
            IndexRepository indexRepository,
            PropertyRepository propertyRepository,
            FeatureRepository featureRepository,
            ModelRepository modelRepository,
            ViewRepository viewRepository,
            ModelSchemaRepository modelSchemaRepository,
            ViewSchemaRepository viewSchemaRepository,
            OFSConfiguration config
    ) {
        this.indexRepository = indexRepository;
        this.propertyRepository = propertyRepository;
        this.modelRepository = modelRepository;
        this.featureRepository = featureRepository;
        this.viewRepository = viewRepository;
        this.modelSchemaRepository = modelSchemaRepository;
        this.viewSchemaRepository = viewSchemaRepository;
        this.config = config;
    }

    /**
     * Exports all graph objects into a given path to a folder.
     * This can later be used to re-assemble the graph via 
     * @see #importOFSState
     * 
     * @param location place to export the graph state to
     * @return
     * @throws InternalException
     * @throws Exception
     */
    public GraphStateResponse exportOFSState(Path location) throws InternalException, Exception {
        GraphStorage ofsGraphStorage = new GraphStorage(location, config.getGraphStorageProvider());
        ExportStateManager exportStateManager = new ExportStateManager(
                ofsGraphStorage,
                indexRepository,
                propertyRepository,
                featureRepository,
                modelRepository,
                viewRepository,
                modelSchemaRepository,
                viewSchemaRepository
        );
        return exportStateManager.exportState();
    }

    /**
     * Imports a graph state from the specified folder path.
     * @see #exportOFSState on how to export the state for later import.
     * 
     * @param location
     * @return
     * @throws InternalException
     * @throws GraphIntegrityException
     * @throws InternalKeywordException
     * @throws Exception
     */
    public GraphStateResponse importOFSState(Path location) throws InternalException, GraphIntegrityException, InternalKeywordException, Exception {
        GraphStorage ofsGraphStorage = new GraphStorage(location, config.getGraphStorageProvider());
        ImportStateManager importStateManager = new ImportStateManager(
                ofsGraphStorage,
                indexRepository,
                propertyRepository,
                featureRepository,
                modelRepository,
                viewRepository,
                modelSchemaRepository,
                viewSchemaRepository,
                config.getImportCacheProvider().getImportCache()
        );
        return importStateManager.importState();
    }

}
