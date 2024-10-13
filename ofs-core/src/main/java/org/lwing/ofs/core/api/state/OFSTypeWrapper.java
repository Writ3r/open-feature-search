/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.api.state;

import java.nio.file.Path;
import org.lwing.ofs.core.api.exception.InternalException;
import org.lwing.ofs.core.api.feature.Feature;
import org.lwing.ofs.core.api.index.Index;
import org.lwing.ofs.core.api.model.Model;
import org.lwing.ofs.core.api.property.JProperty;
import org.lwing.ofs.core.api.property.PrimitivePropertyKey;
import org.lwing.ofs.core.api.property.RefPropertyKey;
import org.lwing.ofs.core.api.schema.FeatureSchema;
import org.lwing.ofs.core.api.schema.ModelSchema;
import org.lwing.ofs.core.api.schema.ViewSchema;
import org.lwing.ofs.core.api.view.View;

/**
 *
 * @author Lucas Wing
 */
public class OFSTypeWrapper {
    
    private OFSTypeWrapper() {
        
    }
    
    protected static final String OFS_INDEX_TYPE = "ofsindex";
    protected static final String OFS_PROP_TYPE = "ofsprop";
    protected static final String OFS_NODE_TYPE = "ofsnode";

    public enum OFSType {

        INDEX("indices", OFS_INDEX_TYPE, Index.class),
        FEATURE("features", OFS_NODE_TYPE, Feature.class),
        MODEL("models", OFS_NODE_TYPE, Model.class),
        MODEL_SCHEMA("model_schemas", OFS_NODE_TYPE, ModelSchema.class),
        VIEW_SCHEMA("view_schemas", OFS_NODE_TYPE, ViewSchema.class),
        FEATURE_SCHEMA("feature_schemas", OFS_NODE_TYPE, FeatureSchema.class),
        VIEW("view", OFS_NODE_TYPE, View.class),
        REF_PROPERTY("ref_properties", OFS_PROP_TYPE, RefPropertyKey.class),
        PRIM_PROPERTY("prim_properties", OFS_PROP_TYPE, PrimitivePropertyKey.class),
        JPROPERTY("properties", OFS_PROP_TYPE, JProperty.class);

        String foldername;
        Class<? extends StatefulResource> clazz;
        String type;

        OFSType(String foldername, String type, Class<? extends StatefulResource> clazz) {
            this.foldername = foldername;
            this.clazz = clazz;
            this.type = type;
        }

        public static OFSType getTypeFromClazz(Class<? extends StatefulResource> clazz) throws InternalException {
            for (OFSType type : OFSType.values()) {
                if (type.getClazz().isAssignableFrom(clazz)) {
                    return type;
                }
            }
            throw new InternalException("Failed to find GraphStorageType from input class");
        }

        public String buildResource(String name) {
            return getFoldername() + "/" + name + "." + getType();
        }

        public Class<? extends StatefulResource> getClazz() {
            return clazz;
        }

        public String getFoldername() {
            return foldername;
        }

        public Path getFolder(Path rootFolder) {
            return rootFolder.resolve(this.foldername);
        }

        public String getType() {
            return type;
        }

    }
}
