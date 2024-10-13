/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.state;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;

/**
 *
 * @author Lucas Wing
 */
public class DependencyResource {
    
    private final String resource;
    
    private final OFSType ofsType;
    
    private final String name;
    
    protected DependencyResource(OFSType ofsType, String name) {
        this.ofsType = ofsType;
        this.resource = ofsType.buildResource(name);
        this.name = name;
    }
    
    public static DependencyResource fromProperty(OFSType type, String property) {
        return new DependencyResource(type, property);
    }
    
    public static DependencyResource fromNodeId(OFSType type, String nodeId) {
        return new DependencyResource(type, nodeId);
    }
    
    public static DependencyResource fromIndex(String indexName) {
        return new DependencyResource(OFSType.INDEX, indexName);
    }
    
    public static DependencyResource fromResource(String resource) {
        String folderName = StringUtils.substringBefore(resource, "/");
        String name = StringUtils.substringBeforeLast(StringUtils.substringAfter(resource, "/"), ".");
        for (OFSType type: OFSType.values()) {
            if (type.getFoldername().equals(folderName)) {
                return new DependencyResource(type, name);
            }
        }
        return null;
    }
    
    public static DependencyResource fromResource(String resource, OFSType type) {
        String name = StringUtils.substringBeforeLast(StringUtils.substringAfter(resource, "/"), ".");
        return new DependencyResource(type, name);
    }
    
    public OFSType getOfsType() {
        return ofsType;
    }

    public String getResource() {
        return resource;
    }

    public String getName() {
        return name;
    }
    
    public static Set<DependencyResource> getNodeResources(OFSType type, Collection<String> nodeIds) {
        return nodeIds.stream().map(nid -> fromNodeId(type, nid)).collect(Collectors.toSet());
    }
    
    public static Set<DependencyResource> getPropertyResources(OFSType type, Collection<String> properties) {
        return properties.stream().map(p -> fromProperty(type, p)).collect(Collectors.toSet());
    }
    
    public static Set<String> getStringResources(Set<DependencyResource> resources) {
        return resources.stream().map(DependencyResource::getResource).collect(Collectors.toSet());
    }
    
}
