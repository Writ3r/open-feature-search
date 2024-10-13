/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.install;

import java.io.IOException;
import org.lwing.ofs.core.api.JGMgntProvider;
import org.lwing.ofs.core.api.config.OFSConfiguration;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.exception.InternalKeywordException;
import org.lwing.ofs.core.api.index.Index;
import org.lwing.ofs.core.api.index.IndexElementType;
import org.lwing.ofs.core.api.index.IndexType;
import org.lwing.ofs.core.impl.OFSRepository;
import org.lwing.ofs.core.impl.SchemaVertexRepository;
import org.lwing.ofs.core.impl.feature.FeatureRepository;
import org.lwing.ofs.core.impl.index.IndexRepository;
import org.lwing.ofs.core.impl.model.ModelRepository;
import org.lwing.ofs.core.impl.property.PropertyRepository;
import org.lwing.ofs.core.impl.property.primitive.PrimitivePropertyRepository;
import org.lwing.ofs.core.impl.schema.SchemaRepository;
import org.lwing.ofs.core.impl.view.ViewRepository;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import static java.util.Map.entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.JanusGraphSchemaElement;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.SchemaStatus;

/**
 *
 * @author Lucas Wing
 */
public class OFSInstaller {

    private final JanusGraph graph;

    private final OFSConfiguration config;

    private final IndexRepository indexRepository;

    public OFSInstaller(JanusGraph graph, OFSConfiguration config, IndexRepository indexRepository) {
        this.graph = graph;
        this.config = config;
        this.indexRepository = indexRepository;
    }

    /**
     * enum with default indices we add during install. These indices speed up
     * searches for fields which a user (or internal methods) would typically
     * search for.
     */
    public enum InstallIndex {
        // used to speed up searches for all nodes which are of a certin OFS type
        NODE_TYPE(
                new Index("_node_type",
                        buildIndexMap(OFSRepository.NODE_TYPE_FIELD),
                        IndexElementType.VERTEX,
                        IndexType.COMPOSITE,
                        false
                )
        ),
        // used to speed up searching for all features which inherit from a specific model
        MODEL_ID(
                new Index("_model_id",
                        buildIndexMap(OFSConfiguration.MODEL_ID),
                        IndexElementType.VERTEX,
                        IndexType.COMPOSITE,
                        false
                )
        ),
        // used for generic searching for all features that inherit from certain models
        INHERITS_FROM_PROP(
                new Index("_inherits_from",
                        buildIndexMap(FeatureRepository.INHERITS_FROM_PROP),
                        IndexElementType.VERTEX,
                        IndexType.COMPOSITE,
                        false
                )
        ),
        // used to speed up SELECT searches for reference properties
        REF_PROP_NAME(
                new Index("_ref_prop_name",
                        buildIndexMap(SchemaRepository.REF_PROP_NAME_EDGE_PROP),
                        IndexElementType.EDGE,
                        IndexType.COMPOSITE,
                        false
                )
        ),
        // used to speed up searches for reference property nodes pointing at certain property names
        PROP_NAME(
                new Index("_prop_name",
                        buildIndexMap(PropertyRepository.PROPERTY_NAME),
                        IndexElementType.VERTEX,
                        IndexType.COMPOSITE,
                        true
                )
        );

        private final Index ix;

        private InstallIndex(Index ix) {
            this.ix = ix;
        }

        public Index getIx() {
            return ix;
        }

    }

    /**
     * Installs all of OFS's property keys, edge labels and indices
     *
     * @throws GraphIntegrityException
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws InternalKeywordException
     * @throws java.io.IOException
     */
    public void install() throws GraphIntegrityException, InterruptedException, ExecutionException, InternalKeywordException, IOException {
        install(new HashSet(Arrays.asList(InstallIndex.values())));
    }

    /**
     * Internal install method, don't use this unless you know what you're doing
     *
     * @param indiciesToInstall
     * @throws GraphIntegrityException
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws InternalKeywordException
     * @throws java.io.IOException
     */
    public void install(Set<InstallIndex> indiciesToInstall) throws GraphIntegrityException, InterruptedException, ExecutionException, InternalKeywordException, IOException {
        config.setAllowInternalFieldActions(true);
        try ( JGMgntProvider mgnt = new JGMgntProvider(graph.openManagement())) {
            JanusGraphManagement management = mgnt.getMgnt();
            SchemaElementLock locker = new SchemaElementLock(management);
            // install shared schema
            locker.lock(management.makePropertyKey(OFSRepository.NODE_TYPE_FIELD)
                    .dataType(String.class).cardinality(Cardinality.SINGLE).make());
            locker.lock(management.makePropertyKey(OFSConfiguration.MODEL_ID)
                    .dataType(String.class).cardinality(Cardinality.SINGLE).make());
            locker.lock(management.makeEdgeLabel(OFSConfiguration.REF_USES_MODEL).multiplicity(Multiplicity.SIMPLE).make());
            // install feature
            locker.lock(management.makePropertyKey(FeatureRepository.INHERITS_FROM_PROP)
                    .dataType(String.class).cardinality(Cardinality.SET).make());
            // install property
            locker.lock(management.makePropertyKey(PropertyRepository.PROPERTY_NAME)
                    .dataType(String.class).cardinality(Cardinality.SINGLE).make());
            locker.lock(management.makePropertyKey(PropertyRepository.REF_CARDINALITY)
                    .dataType(String.class).cardinality(Cardinality.SINGLE).make());
            locker.lock(management.makeEdgeLabel(PropertyRepository.ALLOWABLE_REF_FIELD).multiplicity(Multiplicity.SIMPLE).make());
            locker.lock(management.makePropertyKey(PrimitivePropertyRepository.ALLOWABLE_STRING_FIELD)
                    .dataType(String.class).cardinality(Cardinality.SET).make());
            locker.lock(management.makePropertyKey(PrimitivePropertyRepository.ALLOWABLE_CHARACTER_FIELD)
                    .dataType(Character.class).cardinality(Cardinality.SET).make());
            locker.lock(management.makePropertyKey(PrimitivePropertyRepository.ALLOWABLE_BOOLEAN_FIELD)
                    .dataType(Boolean.class).cardinality(Cardinality.SET).make());
            locker.lock(management.makePropertyKey(PrimitivePropertyRepository.ALLOWABLE_BYTE_FIELD)
                    .dataType(Byte.class).cardinality(Cardinality.SET).make());
            locker.lock(management.makePropertyKey(PrimitivePropertyRepository.ALLOWABLE_SHORT_FIELD)
                    .dataType(Short.class).cardinality(Cardinality.SET).make());
            locker.lock(management.makePropertyKey(PrimitivePropertyRepository.ALLOWABLE_INTEGER_FIELD)
                    .dataType(Integer.class).cardinality(Cardinality.SET).make());
            locker.lock(management.makePropertyKey(PrimitivePropertyRepository.ALLOWABLE_LONG_FIELD)
                    .dataType(Long.class).cardinality(Cardinality.SET).make());
            locker.lock(management.makePropertyKey(PrimitivePropertyRepository.ALLOWABLE_FLOAT_FIELD)
                    .dataType(Float.class).cardinality(Cardinality.SET).make());
            locker.lock(management.makePropertyKey(PrimitivePropertyRepository.ALLOWABLE_DOUBLE_FIELD)
                    .dataType(Double.class).cardinality(Cardinality.SET).make());
            locker.lock(management.makePropertyKey(PrimitivePropertyRepository.ALLOWABLE_DATE_FIELD)
                    .dataType(Date.class).cardinality(Cardinality.SET).make());
            locker.lock(management.makePropertyKey(PrimitivePropertyRepository.ALLOWABLE_GEOSHAPE_FIELD)
                    .dataType(Geoshape.class).cardinality(Cardinality.SET).make());
            locker.lock(management.makePropertyKey(PrimitivePropertyRepository.ALLOWABLE_UUID_FIELD)
                    .dataType(UUID.class).cardinality(Cardinality.SET).make());
            // install view
            locker.lock(management.makeEdgeLabel(ViewRepository.VIEW_SCHEMA).multiplicity(Multiplicity.SIMPLE).make());
            locker.lock(management.makeEdgeLabel(ViewRepository.VIEWS_MODEL_LABEL).multiplicity(Multiplicity.SIMPLE).make());
            locker.lock(management.makeEdgeLabel(ViewRepository.VIEWS_VIEW_LABEL).multiplicity(Multiplicity.SIMPLE).make());
            // install schema
            locker.lock(management.makePropertyKey(SchemaRepository.PRIMITIVE_PROPERTIES)
                    .dataType(String.class).cardinality(Cardinality.SET).make());
            locker.lock(management.makePropertyKey(SchemaRepository.REFERENCE_PROPERTIES)
                    .dataType(String.class).cardinality(Cardinality.SET).make());
            locker.lock(management.makePropertyKey(SchemaRepository.REF_PROP_NAME_EDGE_PROP)
                    .dataType(String.class).cardinality(Cardinality.SET).make());
            locker.lock(management.makeEdgeLabel(SchemaRepository.REF_SCHEMA_DEFALT_EDGE).multiplicity(Multiplicity.MULTI).make());
            locker.lock(management.makeEdgeLabel(SchemaVertexRepository.REF_PROP_EDGE).multiplicity(Multiplicity.MULTI).make());
            locker.lock(management.makeEdgeLabel(SchemaRepository.USED_SCHEMA_PROP_EDGE).multiplicity(Multiplicity.SIMPLE).make());
            // install model
            locker.lock(management.makeEdgeLabel(ModelRepository.MODEL_SCHEMA).multiplicity(Multiplicity.SIMPLE).make());
            locker.lock(management.makeEdgeLabel(ModelRepository.FEATURE_SCHEMA).multiplicity(Multiplicity.SIMPLE).make());
            locker.lock(management.makeEdgeLabel(ModelRepository.EXTENDS_EDGE_ID).multiplicity(Multiplicity.MULTI).make());
            // make indices
            for (InstallIndex ix : indiciesToInstall) {
                indexRepository.createIndex(ix.getIx(), management);
            }
        }
        // wait for indices to register
        for (InstallIndex ix : indiciesToInstall) {
            indexRepository.awaitIndexStatus(ix.getIx().getName(), SchemaStatus.REGISTERED);
        }
        config.setAllowInternalFieldActions(false);
    }

    private class SchemaElementLock {

        JanusGraphManagement management;

        public SchemaElementLock(JanusGraphManagement management) {
            this.management = management;
        }

        // I want the graph schema to be as consistent as possible, so adding these locks should help with that.
        public void lock(JanusGraphSchemaElement jgse) {
            management.setConsistency(jgse, ConsistencyModifier.LOCK);
        }
    }
    
    private static Map<String, Parameter[]> buildIndexMap(String indexKey) {
        Map<String, Parameter[]> paramMap = new HashMap<>();
        paramMap.put(indexKey, null);
        return paramMap;
    }

}
