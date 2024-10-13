/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.lock;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

/**
 *
 * @author Lucas Wing
 */
public class ResourceLockImplTest {
    
    @Test
    public void testLockRemovalDuration() throws InterruptedException {
        String lockResource = "testLock";
        ResourceLockImpl resourceLockImpl = new ResourceLockImpl(3L);
        resourceLockImpl.acquireLocks(new HashSet<>(Arrays.asList(lockResource)));
        assertTrue(resourceLockImpl.isResourceLocked(lockResource));
        await().pollInSameThread().atMost(10, TimeUnit.SECONDS).until(() -> !resourceLockImpl.isResourceLocked(lockResource));
        assertFalse(resourceLockImpl.isResourceLocked(lockResource));
    }
    
    @Test
    public void testLockAndRelease() {
        String lockResource = "testLock";
        String lockResource2 = "testLock2";
        Set<String> resourcesToLock = new HashSet<>(Arrays.asList(lockResource, lockResource2));
        ResourceLockImpl resourceLockImpl = new ResourceLockImpl(3L);
        resourceLockImpl.acquireLocks(new HashSet<>(Arrays.asList(lockResource, lockResource2)));
        assertTrue(resourceLockImpl.isResourceLocked(lockResource));
        assertTrue(resourceLockImpl.isResourceLocked(lockResource2));
        resourceLockImpl.releaseLocks(resourcesToLock);
        assertFalse(resourceLockImpl.isResourceLocked(lockResource));
        assertFalse(resourceLockImpl.isResourceLocked(lockResource2));
    }
    
}
