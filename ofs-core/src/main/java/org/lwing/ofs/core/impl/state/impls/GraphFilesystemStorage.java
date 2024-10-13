/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.state.impls;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.lwing.ofs.core.api.state.ResourceToIdStore;
import org.lwing.ofs.core.api.state.GraphStorageSystem;

/**
 *
 * @author Lucas Wing
 */
public class GraphFilesystemStorage implements GraphStorageSystem {

    @Override
    public DirectoryStream<Path> listFilesInDirectory(Path location, String glob) throws IOException {
        if (!location.toFile().isDirectory()) {
            return new EmptyDirStream();
        }
        return Files.newDirectoryStream(location, glob);
    }

    @Override
    public void storeStringFile(Path location, String input) throws IOException {
        File locDir = location.getParent().toFile();
        if (!locDir.exists()) {
            Files.createDirectory(location.getParent());
        }
        Files.writeString(location, input, StandardOpenOption.CREATE);
    }

    @Override
    public String readStringFile(Path location) throws IOException {
        return Files.readString(location);
    }

    @Override
    public ResourceToIdStore getResourceToFileIdStore(Path location) throws IOException {
        return new ResourceToIdFilesystemStore(location);
    }

    @Override
    public boolean pathExists(Path location) throws IOException {
        return location.toFile().exists();
    }
    
    private class EmptyDirStream implements DirectoryStream<Path> {
        @Override
        public Iterator<Path> iterator() {
            return new Iterator<Path>() {
                @Override
                public boolean hasNext() {
                    return false;
                }
                @Override
                public Path next() {
                    throw new NoSuchElementException("Not supported yet.");
                }
            };
        }
        @Override
        public void close() throws IOException {
            return;
        }
    }

}
