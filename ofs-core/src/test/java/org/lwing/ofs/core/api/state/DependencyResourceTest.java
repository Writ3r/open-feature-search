/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import org.lwing.ofs.core.api.state.OFSTypeWrapper.OFSType;

/**
 *
 * @author Lucas Wing
 */
public class DependencyResourceTest {
    
    @Test
    public void testDependencyResourceFromResource() {
        DependencyResource res = DependencyResource.fromProperty(OFSType.PRIM_PROPERTY, "testProp");
        String resource = res.getResource();
        assertEquals(resource, DependencyResource.fromResource(resource).getResource());
    }
    
    @Test
    public void testDependencyResourceFromResourceAndType() {
        DependencyResource res = DependencyResource.fromProperty(OFSType.PRIM_PROPERTY, "testProp");
        String resource = res.getResource();
        assertEquals(resource, DependencyResource.fromResource(resource, OFSType.PRIM_PROPERTY).getResource());
    }
    
}
