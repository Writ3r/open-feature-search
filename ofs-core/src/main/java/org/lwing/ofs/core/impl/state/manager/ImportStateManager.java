/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.state.manager;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.map.LRUMap;
import static org.awaitility.Awaitility.await;
import org.lwing.ofs.core.api.exception.InternalException;
import org.lwing.ofs.core.api.feature.Feature;
import org.lwing.ofs.core.api.index.Index;
import org.lwing.ofs.core.api.model.Model;
import org.lwing.ofs.core.api.property.PrimitivePropertyKey;
import org.lwing.ofs.core.api.property.RefPropertyKey;
import org.lwing.ofs.core.api.schema.ModelSchema;
import org.lwing.ofs.core.api.schema.ViewSchema;
import org.lwing.ofs.core.api.state.DependencyResource;
import org.lwing.ofs.core.api.state.GraphStateResponse;
import org.lwing.ofs.core.api.state.ImportCacheProvider.ImportCache;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;
import org.lwing.ofs.core.api.state.StatefulResource;
import org.lwing.ofs.core.api.view.View;
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
public class ImportStateManager extends AbsStateManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ImportStateManager.class);

    private static final String IMPORT_LOG_FORMAT = "Importing {} [{}] into graph.";

    private final ImportCache importCache;
    
    private final LRUSet resExistsCache;

    public ImportStateManager(
            GraphStorage ofsGraphStorage,
            IndexRepository indexRepository,
            PropertyRepository propertyRepository,
            FeatureRepository featureRepository,
            ModelRepository modelRepository,
            ViewRepository viewRepository,
            ModelSchemaRepository modelSchemaRepository,
            ViewSchemaRepository viewSchemaRepository,
            ImportCache importCache
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
        this.importCache = importCache;
        this.resExistsCache = new LRUSet(5000);
    }

    public GraphStateResponse importState() throws InternalException, Exception {
        importObjsUnderType(OFSType.PRIM_PROPERTY);
        importObjsUnderType(OFSType.INDEX);
        importObjsUnderType(OFSType.REF_PROPERTY);
        importObjsUnderType(OFSType.MODEL_SCHEMA);
        importObjsUnderType(OFSType.MODEL);
        importObjsUnderType(OFSType.VIEW_SCHEMA);
        importObjsUnderType(OFSType.VIEW);
        importObjsUnderType(OFSType.FEATURE);
        return new GraphStateResponse(failedResources);
    }

    private void importObjsUnderType(OFSType type) throws InternalException, Exception {
        try ( DirectoryStream<Path> stream = ofsGraphStorage.streamPaths(type)) {
            for (Path entry : stream) {
                importObj(entry, type);
            }
        }
    }

    private void importObj(Path objToImport, OFSType type) throws InternalException, Exception {
        // get the resource
        String resource = ofsGraphStorage.calcResourceFromPath(objToImport);
        LOGGER.debug("Looking to import resource {}", resource);
        // check to make sure res isn't already imported
        if (importCache.containsResource(resource)) {
            LOGGER.debug("Resource {} already exists in graph, skipping import", resource);
            return;
        }
        StatefulResource obj = ofsGraphStorage.readObject(objToImport, type);
        // read dependencies
        Set<DependencyResource> dependencies = obj.calcDependencies();
        // import all dependencies first
        for (DependencyResource res : dependencies) {
            LOGGER.debug("Looking to import dependency resource {}", res.getResource());
            // calc out the specific dependency (case to deal with props)
            DependencyResource typeSpecificRes = determineSpecificDependency(res);
            if (typeSpecificRes == null) {
                LOGGER.error("Failed to find specific resource for [{}]. "
                        + "Skipping attempted import.", res.getResource());
                continue;
            }
            // if dependent on self, skip
            String typeSpecificResString = typeSpecificRes.getResource();
            if (typeSpecificResString.equals(resource)) {
                LOGGER.debug("Skipping dependency resource {} import. Dependent on self.", typeSpecificResString);
                continue;
            }
            // run import obj on dependency
            Path fileLocation = ofsGraphStorage.calcResourceToPath(typeSpecificRes);
            if (fileLocation != null) {
                LOGGER.debug("Importing dependency {} to resource {}", typeSpecificResString, resource);
                importObj(fileLocation, typeSpecificRes.getOfsType());
            }
            // wait for resource to exist in graph
            waitForResExists(typeSpecificRes);
        }
        // import current obj since dependencies were imported
        LOGGER.debug("All dependencies have been imported for resource {}, importing resource.", resource);
        importObjIntoGraph(obj);
    }

    private void importObjIntoGraph(StatefulResource obj) {
        DependencyResource res = obj.calcResource();
        try {
            switch (res.getOfsType()) {
                case PRIM_PROPERTY -> {
                    LOGGER.debug(IMPORT_LOG_FORMAT, "primitive property", res.getName());
                    propertyRepository.createProperty((PrimitivePropertyKey) obj);
                }
                case REF_PROPERTY -> {
                    LOGGER.debug(IMPORT_LOG_FORMAT, "ref property", res.getName());
                    propertyRepository.createProperty((RefPropertyKey) obj);
                }
                case INDEX -> {
                    LOGGER.debug(IMPORT_LOG_FORMAT, "index", res.getName());
                    indexRepository.createIndex((Index) obj, true);
                }
                case MODEL_SCHEMA -> {
                    LOGGER.debug(IMPORT_LOG_FORMAT, "model schema", res.getName());
                    modelSchemaRepository.createSchema((ModelSchema) obj);
                }
                case VIEW_SCHEMA -> {
                    LOGGER.debug(IMPORT_LOG_FORMAT, "view schema", res.getName());
                    viewSchemaRepository.createSchema((ViewSchema) obj);
                }
                case MODEL -> {
                    LOGGER.debug("Importing model [{}] into graph", res.getName());
                    modelRepository.createModel((Model) obj);
                }
                case VIEW -> {
                    LOGGER.debug(IMPORT_LOG_FORMAT, "view", res.getName());
                    viewRepository.createView((View) obj);
                }
                case FEATURE -> {
                    LOGGER.debug(IMPORT_LOG_FORMAT, "feature", res.getName());
                    featureRepository.addFeature((Feature) obj);
                }
            }
        } catch (Exception ex) {
            LOGGER.warn("Failed to import object into Graph", ex);
            failedResources.add(obj.calcResource().getResource());
        }
        importCache.storeResource(res.getResource());
    }

    private DependencyResource determineSpecificDependency(DependencyResource res) throws IOException, InternalException {
        // JProps don't actually exist, so need to find the ACTUAL type of the JProp
        if (res.getOfsType().equals(OFSType.JPROPERTY)) {
            return findActualResource(
                    DependencyResource.fromProperty(OFSType.PRIM_PROPERTY, res.getName()),
                    DependencyResource.fromProperty(OFSType.REF_PROPERTY, res.getName())
            );
        }
        // just return the resource if it's not a JProp
        return res;
    }

    private DependencyResource findActualResource(DependencyResource... possibleResources) throws IOException, InternalException {
        // first try to check cache since that's faster than a filesystem check
        for (DependencyResource res : possibleResources) {
            if (importCache.containsResource(res.getResource())) {
                return res;
            }
        }
        // cache check failed, check filesystem
        for (DependencyResource res : possibleResources) {
            if (ofsGraphStorage.objectExists(res.getResource())) {
                return res;
            }
        }
        // should never hit this case
        return null;
    }

    private void waitForResExists(DependencyResource res) {
        await().pollInSameThread().atMost(180, TimeUnit.SECONDS)
                .until(() -> checkResExists(res));
    }

    private boolean checkResExists(DependencyResource res) throws Exception {
        LOGGER.debug("Checking if resource exists {}", res.getResource());
        // check cache, if exists exit early
        if (resExistsCache.containsKey(res.getResource())) {
            return true;
        }
        // cache miss, proceeed to check graph for res
        boolean resExists;
        switch (res.getOfsType()) {
            case PRIM_PROPERTY, REF_PROPERTY -> {
                resExists =  propertyRepository.containsProperty(res.getName());
            }
            case INDEX -> {
                resExists = indexRepository.containsIndex(res.getName());
            }
            case MODEL_SCHEMA, VIEW_SCHEMA, MODEL, VIEW, FEATURE -> {
                resExists = featureRepository.vertexExists(res.getName());
            }
            default -> {
                LOGGER.warn("Failed to determine resource lookup for type {}", res.getOfsType());
                resExists = false;
            }
        }
        // populate cache with result if exists
        if (resExists) {
            resExistsCache.add(res.getResource());
        }
        return resExists;
    }

    // simple wrapper for map to act like a set
    private class LRUSet {

        private final LRUMap map;

        public LRUSet(Integer size) {
            map = new LRUMap(size);
        }

        public boolean containsKey(String key) {
            boolean keyExists = map.containsKey(key);
            if (keyExists) {
                map.get(key); // exec get for LRU to activate

            }
            return keyExists;
        }

        public void add(String key) {
            map.put(key, null);
        }

    }

}
