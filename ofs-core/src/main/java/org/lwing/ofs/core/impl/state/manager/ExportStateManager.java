/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.state.manager;

import java.io.IOException;
import java.util.Collection;
import org.lwing.ofs.core.api.OFSIdVertex;
import org.lwing.ofs.core.api.exception.InternalException;
import org.lwing.ofs.core.api.state.GraphStateResponse;
import org.lwing.ofs.core.api.state.StatefulResource;
import org.lwing.ofs.core.impl.SearchableVertexRepository;
import org.lwing.ofs.core.impl.feature.FeatureRepository;
import org.lwing.ofs.core.impl.index.IndexRepository;
import org.lwing.ofs.core.impl.model.ModelRepository;
import org.lwing.ofs.core.impl.model.ModelSchemaRepository;
import org.lwing.ofs.core.impl.property.PropertyRepository;
import org.lwing.ofs.core.impl.state.storage.GraphStorage;
import org.lwing.ofs.core.impl.view.ViewRepository;
import org.lwing.ofs.core.impl.view.ViewSchemaRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Lucas Wing
 */
public class ExportStateManager extends AbsStateManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExportStateManager.class);

    public ExportStateManager(
            GraphStorage ofsGraphStorage,
            IndexRepository indexRepository,
            PropertyRepository propertyRepository,
            FeatureRepository featureRepository,
            ModelRepository modelRepository,
            ViewRepository viewRepository,
            ModelSchemaRepository modelSchemaRepository,
            ViewSchemaRepository viewSchemaRepository
    ) {
        super(
                ofsGraphStorage,
                indexRepository,
                propertyRepository,
                featureRepository,
                modelRepository,
                viewRepository,
                modelSchemaRepository,
                viewSchemaRepository
        );
    }

    public GraphStateResponse exportState() throws Exception {
        addObjsFromSearch(featureRepository);
        addObjsFromSearch(viewRepository);
        addObjsFromSearch(modelRepository);
        addObjsFromSearch(modelSchemaRepository);
        addObjsFromSearch(viewSchemaRepository);
        exportObjects(indexRepository.listIndices());
        exportObjects(propertyRepository.listProperties());
        return new GraphStateResponse(failedResources);
    }

    private <E extends StatefulResource & OFSIdVertex> void addObjsFromSearch(SearchableVertexRepository<E> repo) throws Exception {
        repo.search(g -> {
            return g.V();
        }, outIterator -> {
            while (outIterator.hasNext()) {
                E outObj = outIterator.next();
                try {
                    ofsGraphStorage.storeObject(outObj);
                } catch (Exception ex) {
                    LOGGER.warn("Failed to store object", ex);
                    failedResources.add(outObj.calcResource().getResource());
                }
            }
        });
    }

    private <E extends StatefulResource> void exportObjects(Collection<E> objs) throws InternalException, IOException {
        for (E obj : objs) {
            ofsGraphStorage.storeObject(obj);
        }
    }

}
