/*
 * Copyright (C) 2024 Lucas Wing
 * The Man, The Myth, The Legend.
 */
package org.lwing.ofs.core.impl;

import org.lwing.ofs.core.api.search.GraphSearch;
import org.lwing.ofs.core.api.search.GraphSearchResponseHandler;
import org.lwing.ofs.core.api.config.OFSConfiguration;
import org.lwing.ofs.core.api.VertexIterator;
import org.lwing.ofs.core.api.VertexType;
import org.lwing.ofs.core.api.exception.GraphSearchException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.lwing.ofs.core.api.OFSIdVertex;

/**
 *
 * @author Lucas Wing
 * @param <E> type of vertex that's searchable
 */
public abstract class SearchableVertexRepository<E extends OFSIdVertex> extends VertexRepository {

    protected SearchableVertexRepository(JanusGraph graph, OFSConfiguration config, VertexType vertexType) {
        super(graph, config, vertexType);
    }
    
    /**
     * Executes a search operation and returns the result in a list. Be careful
     * when using this method because the results are all held in memory.
     *
     * @param graphSearch search to be executed
     * @param select optional set of fields to select in the results, empty
     * optional means all fields
     * @return list of results from the search
     * @throws GraphSearchException OFS related exceptions
     * @throws Exception JanusGraph generic exception
     */
    public List<E> search(GraphSearch graphSearch, Optional<Set<String>> select) throws GraphSearchException, Exception {
        List<E> responseList = new ArrayList<>();
        search(graphSearch, featureIterator -> {
            while (featureIterator.hasNext()) {
                responseList.add(featureIterator.next());
            }
        }, select);
        return responseList;
    }

    /**
     * Executes a search operation and returns the result in a list. Be careful
     * when using this method because the results are all held in memory.
     *
     * @param graphSearch search to be executed
     * @return list of results from the search
     * @throws GraphSearchException OFS related exceptions
     * @throws Exception JanusGraph generic exception
     */
    public List<E> search(GraphSearch graphSearch) throws GraphSearchException, Exception {
        return search(graphSearch, PropertyUtil.ALL_SELECT);
    }

    /**
     * Executes a search operation with all results being fed into the
     * responseHandler. The search operation is carried out via the GraphSearch
     * while a separate non-type specific GraphTraversalSource is used to
     * execute the load (so linked objects like Model Schemas can be picked up).
     * You can apply your own SubgraphStrategy to the GraphTraversalSource in
     * your provided graphSearch to handle Use Cases like user-permissions.
     *
     * info: great ex. query tool https://gremlify.com/
     * https://groups.google.com/g/gremlin-users/c/WmgvwjRPux0/m/mWJh5SmSCAAJ
     * https://stackoverflow.com/questions/71161247/in-gremlin-how-can-i-use-the-subgraphstrategy-when-submitting-a-script
     *
     * @param graphSearch search to be executed
     * @param responseHandler handles the search results
     * @param select optional of a set of fields to select in the response
     * object. Empty optional means all fields.
     * @throws GraphSearchException OFS related exceptions
     * @throws Exception JanusGraph generic exception
     */
    public void search(GraphSearch graphSearch, GraphSearchResponseHandler<E> responseHandler, Optional<Set<String>> select) throws GraphSearchException, Exception {
        // do a search traversal (aka only search for the nodes that are of this type & fit user input strategies)
        try ( GraphTraversalSource g = getSearchTraversalSource()) {
            // also need a more general traversal source to also resolve nodes of other types like schemas when building objs like Models
            try ( GraphTraversalSource rog = getReadOnlyTraversalSource()) {
                responseHandler.handleResponse(new VertexIterator<E>(graphSearch.search(g), g) {
                    @Override
                    public E next() {
                        return buildType(traversal.next(), rog, select);
                    }
                });
            }
        }
    }

    /**
     * Executes a search operation with all results being fed into the
     * responseHandler. The search operation is carried out via the GraphSearch
     * while a separate non-type specific GraphTraversalSource is used to
     * execute the load (so linked objects like Model Schemas can be picked up).
     * You can apply your own SubgraphStrategy to the GraphTraversalSource in
     * your provided graphSearch to handle Use Cases like user-permissions.
     *
     * info: great ex. query tool https://gremlify.com/
     * https://groups.google.com/g/gremlin-users/c/WmgvwjRPux0/m/mWJh5SmSCAAJ
     * https://stackoverflow.com/questions/71161247/in-gremlin-how-can-i-use-the-subgraphstrategy-when-submitting-a-script
     *
     * @param graphSearch search to be executed
     * @param responseHandler handles the search results
     * @throws GraphSearchException OFS related exceptions
     * @throws Exception JanusGraph generic exception
     */
    public void search(GraphSearch graphSearch, GraphSearchResponseHandler<E> responseHandler) throws GraphSearchException, Exception {
        search(graphSearch, responseHandler, PropertyUtil.ALL_SELECT);
    }
    
    /**
     * Builds the type of object that this repository primarily supports. For
     * example, it builds the Model object for the ModelRepository. This is
     * required so Search can build returnable objects.
     *
     * @param v vertex we intend to build the object off of
     * @param g traversal source that can be used for looking up additional
     * information
     * @param select optional set of select fields, if the optional is empty
     * selects all fields
     * @return main object supported by this repository
     */
    protected abstract E buildType(Vertex v, GraphTraversalSource g, Optional<Set<String>> select);
    
    private GraphTraversalSource getSearchTraversalSource() {
        return getReadOnlyTraversalSource()
                .withStrategies(SubgraphStrategy.build()
                        .vertices(has(OFSRepository.NODE_TYPE_FIELD, vertexType.name()))
                        .create()
                );
    }

    private GraphTraversalSource getReadOnlyTraversalSource() {
        return getTraversalSource()
                .withStrategies(ReadOnlyStrategy.instance());
    }
    
}
