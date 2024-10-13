/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.state;

import java.io.IOException;

/**
 * When exporting a graph we use the filename of each file to save the exported resource.
 * This can be a problem if the resource contains chars not supported by the filesystem.
 * Thus we save the actual filenames as uuids and map them to the resource via this file.
 * 
 * @author Lucas Wing
 */
public interface ResourceToIdStore {
    
    public void storeResourceToFileSafeId(String resource, String fileSafeId) throws IOException;
    
    /**
     * @param fileSafeId
     * @return The resource from the file safe id. null if it does not exist.
     */
    public String getResourceFromFileSafeId(String fileSafeId);
    
    /**
     * @param resource
     * @return The file safe id for a resource. null if it does not exist.
     */
    public String getFileSafeIdFromResource(String resource);
    
}
