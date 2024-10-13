/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.state;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;

/**
 * System used to interface with for export/import of graph state.
 * The default impl is the local filesystem.
 * 
 * @author Lucas Wing
 */
public interface GraphStorageSystem {

    void storeStringFile(Path location, String input) throws IOException;
    
    String readStringFile(Path location) throws IOException;
    
    boolean pathExists(Path location) throws IOException;
    
    public DirectoryStream<Path> listFilesInDirectory(Path location, String glob) throws IOException;
    
    public ResourceToIdStore getResourceToFileIdStore(Path location) throws IOException;
}
