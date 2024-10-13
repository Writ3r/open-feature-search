/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.state.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.lwing.ofs.core.api.MappingUtil;
import org.lwing.ofs.core.api.exception.InternalException;
import org.lwing.ofs.core.api.state.DependencyResource;
import org.lwing.ofs.core.api.state.StatefulResource;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;
import org.lwing.ofs.core.api.state.ResourceToIdStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.lwing.ofs.core.api.state.GraphStorageSystem;

/**
 * 
 * @author Lucas Wing
 */
public class GraphStorage {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphStorage.class);

    private final GraphStorageSystem ofsGraphStorageProvider;

    private final Path stateLocation;
    
    private final ResourceToIdStore resourceToIdStore;

    public GraphStorage(Path stateLocation, GraphStorageSystem ofsGraphStorageProvider) throws IOException {
        this.ofsGraphStorageProvider = ofsGraphStorageProvider;
        this.stateLocation = stateLocation;
        this.resourceToIdStore = ofsGraphStorageProvider.getResourceToFileIdStore(stateLocation);
    }

    public <E extends StatefulResource> E readObject(Path objToImport, OFSType type) throws IOException, InternalException {
        String fileIn = ofsGraphStorageProvider.readStringFile(objToImport);
        return MappingUtil.jsonStringToObject(fileIn, (Class<E>) type.getClazz());
    } 
    
    public boolean objectExists(String resource) throws IOException, InternalException {
        return getFileIdFromResource(resource) != null;
    } 

    public DirectoryStream<Path> streamPaths(OFSType type) throws IOException {
        return ofsGraphStorageProvider.listFilesInDirectory(type.getFolder(stateLocation), "*" + type.getType());
    }
    
    public void storeObject(StatefulResource resource) throws InternalException, IOException {
        String objOut = MappingUtil.turnObjToJsonString(resource);
        DependencyResource res = resource.calcResource();
        String objResource = res.getResource();
        String fileName = getFileSafeId(objResource);
        Path fileLocation = res.getOfsType().getFolder(stateLocation)
                .resolve(fileName + "." + res.getOfsType().getType());
        ofsGraphStorageProvider.storeStringFile(fileLocation, objOut);
        resourceToIdStore.storeResourceToFileSafeId(objResource, fileName);
    }
    
    public String getResourceFromFileId(String fileId) throws IOException {
        return resourceToIdStore.getResourceFromFileSafeId(fileId);
    }
    
    public String getFileIdFromResource(String resource) throws IOException {
        return resourceToIdStore.getFileSafeIdFromResource(resource);
    }
    
    public String calcResourceFromPath(Path path) throws IOException {
        String filename = path.getFileName().toString();
        String fileNameId = StringUtils.substringBeforeLast(filename, ".");
        return getResourceFromFileId(fileNameId);
    }

    public Path calcResourceToPath(DependencyResource res) throws IOException {
        String fileId = getFileIdFromResource(res.getResource());
        if (fileId == null) {
            LOGGER.error("Failed to find fileId for associated resource [{}]. "
                    + "This will prevent import of that object.", res.getResource());
            return null;
        }
        return res.getOfsType().getFolder(getStateLocation())
                .resolve(fileId + "." + res.getOfsType().getType());
    }
        
    private String getFileSafeId(String objResource) {
        return UUID.nameUUIDFromBytes(objResource.getBytes()).toString().replace("-", "");
    }
    
    public Path getStateLocation() {
        return stateLocation;
    }
}
