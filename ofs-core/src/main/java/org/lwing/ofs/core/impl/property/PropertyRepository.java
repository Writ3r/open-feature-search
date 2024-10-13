/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl.property;

import org.lwing.ofs.core.impl.property.reference.ReferencePropertyRepository;
import org.lwing.ofs.core.impl.property.primitive.PrimitivePropertyRepository;
import org.lwing.ofs.core.api.config.OFSConfiguration;
import static org.lwing.ofs.core.api.config.OFSConfiguration.INTERNAL_FIELD_PREFIX;
import org.lwing.ofs.core.api.exception.GraphIntegrityException;
import org.lwing.ofs.core.api.exception.InternalKeywordException;
import org.lwing.ofs.core.api.property.JProperty;
import org.lwing.ofs.core.api.property.PrimitivePropertyKey;
import org.lwing.ofs.core.api.property.RefPropertyKey;
import org.lwing.ofs.core.impl.OFSRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.janusgraph.core.JanusGraph;
import org.lwing.ofs.core.api.JGMgntProvider;

/**
 *
 * @author Lucas Wing
 */
public class PropertyRepository extends OFSRepository {

    // field on ref property specifying the associated property name
    public static final String PROPERTY_NAME = INTERNAL_FIELD_PREFIX + "property_name";

    // field to save cardinality val on a ref node
    public static final String REF_CARDINALITY = INTERNAL_FIELD_PREFIX + "ref_cardinality";

    // allowable value fields
    public static final String ALLOWABLE_REF_FIELD = INTERNAL_FIELD_PREFIX + "allowable_ref_value";

    private final ReferencePropertyRepository refPropRepo;

    private final PrimitivePropertyRepository primPropRepo;

    public PropertyRepository(JanusGraph graph, OFSConfiguration config) {
        super(graph, config);
        this.primPropRepo = new PrimitivePropertyRepository(graph, config);
        this.refPropRepo = new ReferencePropertyRepository(graph, config);
    }

    /**
     * Creates a property definition which allows for creating references to
     * features which inherit from the stated Model object.
     *
     * @param property reference property to create
     * @throws InternalKeywordException if the property being created is
     * considered internal
     * @throws GraphIntegrityException if the vertex id being passed in for the
     * model is not a model
     * @throws Exception generic JanusGraph exception
     */
    public void createProperty(RefPropertyKey property) throws InternalKeywordException, GraphIntegrityException, Exception {
        refPropRepo.createProperty(property);
    }

    /**
     * Creates a primitive property definition which allows for adding primitive
     * JanusGraph properties on your schemas.
     *
     * @param property
     * @throws GraphIntegrityException
     * @throws Exception
     */
    public void createProperty(PrimitivePropertyKey property) throws GraphIntegrityException, Exception {
        primPropRepo.createProperty(property);
    }

    /**
     * Checks if the graph contains the specified property
     *
     * @param property
     * @return
     */
    public boolean containsProperty(String property) {
        try ( JGMgntProvider mgnt = useManagement()) {
            return mgnt.getMgnt().containsPropertyKey(property);
        }
    }

    /**
     * Retrieves a property's information from the graph. This can be either a
     * RefPropertyKey or PrimitivePropertyKey.
     *
     * @param name property name to lookup
     * @return the property if it exists
     * @throws GraphIntegrityException if the property does not exist
     * @throws Exception generic JanusGraph exception
     */
    public JProperty getProperty(String name) throws GraphIntegrityException, Exception {
        Optional<RefPropertyKey> refKey = readRefProperty(name);
        if (refKey.isPresent()) {
            return refKey.get();
        }
        Optional<PrimitivePropertyKey> primKey = readPrimitiveProperty(name);
        if (primKey.isPresent()) {
            return primKey.get();
        }
        throw new GraphIntegrityException("Property [%s] does not exist", name);
    }

    /**
     * List all Primitive and Reference properties that exist
     *
     * @return list of all PrimitivePropertyKey and RefPropertyKey
     * @throws Exception generic Janusgraph exception
     */
    public List<JProperty> listProperties() throws Exception {
        ArrayList<JProperty> outProps = new ArrayList<>();
        outProps.addAll(listPrimitiveProps());
        outProps.addAll(listRefProps());
        return outProps;
    }

    /**
     * Retrieve a list of all primitive properties in the system
     *
     * @return list of all primitive properties
     * @throws Exception generic JanusGraph exception
     */
    public List<PrimitivePropertyKey> listPrimitiveProps() throws Exception {
        return primPropRepo.listPrimitiveProps();
    }

    /**
     * Retrieve a list of all reference properties in the system
     *
     * @return list of all reference properties
     * @throws Exception generic JanusGraph exception
     */
    public List<RefPropertyKey> listRefProps() throws Exception {
        return refPropRepo.listRefProps();
    }

    /**
     * Reads a primitive property & returns a populated optional if it exists.An
     * empty optional is returned if it does not.
     *
     * @param name property to lookup the object for
     * @return Optional of the looked up object
     * @throws java.lang.Exception generic JanusGraph exception
     */
    public Optional<PrimitivePropertyKey> readPrimitiveProperty(String name) throws Exception {
        return primPropRepo.readPrimitiveProperty(name);
    }

    /**
     * Reads a reference property & returns a populated optional if it exists.
     * An empty optional is returned if it does not.
     *
     * @param name property to lookup the object for
     * @return Optional of the looked up object
     * @throws Exception generic JanusGraph exception
     */
    public Optional<RefPropertyKey> readRefProperty(String name) throws Exception {
        return refPropRepo.readRefProperty(name);
    }

    /**
     * Checks if a property name exists as a Primitive or Reference property
     *
     * @param name property to check
     * @return if the property exists
     * @throws Exception generic JanusGraph exception
     */
    public boolean propertyExists(String name) throws Exception {
        return readRefProperty(name).isPresent() || readPrimitiveProperty(name).isPresent();
    }

    /**
     * Deletes a property record. Keep in mind, in JanusGraph deleting
     * Properties isn't a thing, you have to rename them, as per:
     * https://docs.janusgraph.org/schema/ So we just rename the property to a
     * random UUID and return
     *
     * @param name property name to delete
     * @return re-named property key (random uuid)
     * @throws GraphIntegrityException if the property is being referenced by a
     * schema
     * @throws Exception generic JanusGraph exception
     */
    public String deleteProperty(String name) throws GraphIntegrityException, Exception {
        return primPropRepo.deleteProperty(name);
    }

}
