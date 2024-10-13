/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.lock;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.lwing.ofs.core.api.lock.ResourceLock;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @author Lucas Wing
 */
public class ResourceLockImpl implements ResourceLock {

    private final Map<String, ReentrantLock> lockMap;
    private final Map<String, Instant> resourceToTimeLockedMap;
    private final long maxLockedTimeSeconds;

    public ResourceLockImpl() {
        this.lockMap = new ConcurrentHashMap<>();
        this.resourceToTimeLockedMap = new ConcurrentHashMap<>();
        this.maxLockedTimeSeconds = 120;
    }

    public ResourceLockImpl(Long maxLockedTimeSeconds) {
        this.lockMap = new ConcurrentHashMap<>();
        this.resourceToTimeLockedMap = new ConcurrentHashMap<>();
        this.maxLockedTimeSeconds = maxLockedTimeSeconds;
    }

    @Override
    public synchronized void acquireLocks(Set<String> resources) {
        for (var resource : resources) {
            ReentrantLock lock;
            lock = lockMap.getOrDefault(resource, new ReentrantLock());
            unlockIfPastMaxElapsedTime(resource, lock);
            lock.lock(); // NOSONAR
            lockMap.put(resource, lock);
            resourceToTimeLockedMap.put(resource, Instant.now());
        }
    }

    @Override
    public synchronized void releaseLocks(Set<String> resources) {
        for (var resource : resources) {
            if (lockMap.containsKey(resource)) {
                ReentrantLock lock = lockMap.get(resource);
                lock.unlock();
                lockMap.remove(resource);
                resourceToTimeLockedMap.remove(resource);
            }
        }
    }
    
    protected boolean isResourceLocked(String resource) {
        ReentrantLock lock = lockMap.getOrDefault(resource, new ReentrantLock());
        unlockIfPastMaxElapsedTime(resource, lock);
        return lock.isLocked();
    }

    private void unlockIfPastMaxElapsedTime(String resource, ReentrantLock lock) {
        if (lock.isLocked()) {
            Instant currentTime = Instant.now();
            Instant lockedTime = resourceToTimeLockedMap.getOrDefault(resource, currentTime);
            Long elapsedSeconds = ChronoUnit.SECONDS.between(lockedTime, currentTime);
            if (elapsedSeconds > maxLockedTimeSeconds) {
                lock.unlock();
            }
        }
    }

}
