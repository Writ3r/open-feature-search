/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.state.impls;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;
import org.apache.commons.collections.BidiMap;
import org.apache.commons.collections.bidimap.DualHashBidiMap;
import org.apache.commons.lang3.StringUtils;
import org.lwing.ofs.core.api.state.ResourceToIdStore;

/**
 *
 * @author Lucas Wing
 */
public class ResourceToIdFilesystemStore implements ResourceToIdStore {

    private final BidiMap resourceToFileSafeIdMap;

    private final Path resToFieldPath;

    private static final String RESOURCE_TO_FILE_FILE = "resToField.properties";

    private static final String KEY_VAL_SEP = "=";

    public ResourceToIdFilesystemStore(Path location) throws IOException {
        this.resToFieldPath = location.resolve(RESOURCE_TO_FILE_FILE);
        this.resourceToFileSafeIdMap = initializeResourceCache();
    }

    @Override
    public void storeResourceToFileSafeId(String resource, String fileSafeId) throws IOException {
        String input = resource + KEY_VAL_SEP + fileSafeId + System.lineSeparator();
        Files.writeString(resToFieldPath, input, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
    }

    @Override
    public String getResourceFromFileSafeId(String fileSafeId) {
        return (String) this.resourceToFileSafeIdMap.inverseBidiMap().getOrDefault(fileSafeId, null);
    }

    @Override
    public String getFileSafeIdFromResource(String resource) {
        return (String) this.resourceToFileSafeIdMap.getOrDefault(resource, null);
    }

    private BidiMap initializeResourceCache() throws IOException {
        BidiMap resMap = new DualHashBidiMap();
        if (resToFieldPath.toFile().exists()) {
            try ( Stream<String> stream = Files.lines(resToFieldPath)) {
                stream.forEach(line -> {
                    String resource = StringUtils.substringBeforeLast(line, KEY_VAL_SEP);
                    String fileId = StringUtils.substringAfterLast(line, KEY_VAL_SEP);
                    resMap.put(resource, fileId);
                });
            }
        }
        return resMap;
    }

}
