/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.property;

/**
 * Wrapper around JanusGraph's cardinality object This exists in
 * case if more cardinality is added in the future, we won't add it here until
 * we support that cardinality type in our JProperties.
 *
 * @author Lucas Wing
 */
public enum Cardinality {

    SINGLE(org.janusgraph.core.Cardinality.SINGLE),
    LIST(org.janusgraph.core.Cardinality.LIST),
    SET(org.janusgraph.core.Cardinality.SET);

    private final org.janusgraph.core.Cardinality jCardinality;

    private Cardinality(org.janusgraph.core.Cardinality cardinality) {
        this.jCardinality = cardinality;
    }

    public org.janusgraph.core.Cardinality getCardinality() {
        return jCardinality;
    }

    public static Cardinality valueFrom(org.janusgraph.core.Cardinality inputCardinality) {
        return Cardinality.valueOf(inputCardinality.name());
    }

}
