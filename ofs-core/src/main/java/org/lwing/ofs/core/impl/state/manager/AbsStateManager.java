/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.state.manager;

import java.util.HashSet;
import java.util.Set;
import org.lwing.ofs.core.impl.feature.FeatureRepository;
import org.lwing.ofs.core.impl.index.IndexRepository;
import org.lwing.ofs.core.impl.model.ModelRepository;
import org.lwing.ofs.core.impl.model.ModelSchemaRepository;
import org.lwing.ofs.core.impl.property.PropertyRepository;
import org.lwing.ofs.core.impl.state.storage.GraphStorage;
import org.lwing.ofs.core.impl.view.ViewRepository;
import org.lwing.ofs.core.impl.view.ViewSchemaRepository;

/**
 *
 * @author Lucas Wing
 */
public abstract class AbsStateManager {

    protected final IndexRepository indexRepository;

    protected final PropertyRepository propertyRepository;

    protected final FeatureRepository featureRepository;

    protected final ModelRepository modelRepository;

    protected final ViewRepository viewRepository;

    protected final ModelSchemaRepository modelSchemaRepository;

    protected final ViewSchemaRepository viewSchemaRepository;

    protected final GraphStorage ofsGraphStorage;
    
    protected Set<String> failedResources;
    
    protected AbsStateManager(
            GraphStorage ofsGraphStorage,
            IndexRepository indexRepository,
            PropertyRepository propertyRepository,
            FeatureRepository featureRepository,
            ModelRepository modelRepository,
            ViewRepository viewRepository,
            ModelSchemaRepository modelSchemaRepository,
            ViewSchemaRepository viewSchemaRepository
    ) {
        this.indexRepository = indexRepository;
        this.propertyRepository = propertyRepository;
        this.modelRepository = modelRepository;
        this.featureRepository = featureRepository;
        this.viewRepository = viewRepository;
        this.modelSchemaRepository = modelSchemaRepository;
        this.viewSchemaRepository = viewSchemaRepository;
        this.ofsGraphStorage = ofsGraphStorage;
        this.failedResources = new HashSet<>();
    }

}
