package eu.unifiedviews.plugins.transformer.fusiontool.io;

import cz.cuni.mff.odcleanstore.fusiontool.config.LDFTConfigConstants;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.data.AllTriplesLoader;
import cz.cuni.mff.odcleanstore.fusiontool.util.LDFusionToolUtils;
import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.rdf.RDFDataUnit;
import org.openrdf.OpenRDFException;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

import static cz.cuni.mff.odcleanstore.fusiontool.config.LDFTConfigConstants.LOG_LOOP_SIZE;

public class AllTriplesDataUnitLoader implements AllTriplesLoader {
    private static final Logger LOG = LoggerFactory.getLogger(AllTriplesDataUnitLoader.class);

    /**
     * SPARQL query that gets all quads from named graphs optionally limited by named graph restriction pattern.
     * Must be formatted with arguments:
     * (1) namespace prefixes declaration
     * (2) named graph restriction pattern
     * (3) named graph restriction variable
     * (4) result size limit
     * (5) result offset
     */
    private static final String LOAD_SPARQL_QUERY =
            " SELECT ?s ?p ?o"
            + "\n WHERE {"
            + "\n   GRAPH <%1s> {"
            + "\n     ?s ?p ?o"
            + "\n   }"
            + "\n }"
            + "\n LIMIT %2$s OFFSET %3$s";

    private RepositoryConnection _connection;
    private final URI defaultContext;
    private final RDFDataUnit rdfInput;
    private int maxSparqlResultsSize = LDFTConfigConstants.DEFAULT_SPARQL_RESULT_MAX_ROWS;

    public AllTriplesDataUnitLoader(RDFDataUnit rdfInput) throws DataUnitException {
        RDFDataUnit.Iteration defaultContextIteration = rdfInput.getIteration();
        defaultContext = defaultContextIteration.hasNext()
                ? defaultContextIteration.next().getDataGraphURI()
                : null;
        defaultContextIteration.close();
        this.rdfInput = rdfInput;
    }

    public void setMaxSparqlResultsSize(int maxSparqlResultsSize) {
        this.maxSparqlResultsSize = maxSparqlResultsSize;
    }

    @Override
    public void loadAllTriples(RDFHandler rdfHandler) throws LDFusionToolException {
        LOG.info("Loading input quads from data unit");
        RDFDataUnit.Iteration contextsIteration = null;
        try {
            rdfHandler.startRDF();
            long totalStartTime = System.currentTimeMillis();
            int totalLoadedQuads = 0;
            contextsIteration = rdfInput.getIteration();
            while (contextsIteration.hasNext()) {
                totalLoadedQuads += loadAllTriplesForGraph(contextsIteration.next().getDataGraphURI(), rdfHandler);
            }
            rdfHandler.endRDF();
            LOG.info("Loaded {} input quads from data unit in {}", totalLoadedQuads,  LDFusionToolUtils.formatTime(System.currentTimeMillis() - totalStartTime));
        } catch (RDFHandlerException e) {
            throw new LDFusionToolException("Error processing quads from data unit: " + e.getMessage(), e);
        } catch (DataUnitException | OpenRDFException e) {
            throw new LDFusionToolException("Error loading quads from data unit: " + e.getMessage(), e);
        } finally {
            if (contextsIteration != null) {
                try {
                    contextsIteration.close();
                } catch (DataUnitException e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
    }

    private int loadAllTriplesForGraph(URI dataGraphURI, RDFHandler rdfHandler) throws OpenRDFException, DataUnitException {
        LOG.debug("Loading input quads from data unit for graph {}", dataGraphURI);
        int totalLoadedQuads = 0;
        int lastLoadedQuads = Integer.MAX_VALUE;
        for (int offset = 0; lastLoadedQuads >= maxSparqlResultsSize; offset += lastLoadedQuads) {
            String query = formatQuery(dataGraphURI, maxSparqlResultsSize, offset);
            lastLoadedQuads = addQuadsFromQuery(dataGraphURI, query, rdfHandler);
            totalLoadedQuads += lastLoadedQuads;

            if ((totalLoadedQuads - lastLoadedQuads) / LOG_LOOP_SIZE != totalLoadedQuads / LOG_LOOP_SIZE) {
                LOG.info("ODCS-FusionTool: Loaded {} quads from graph {} so far", totalLoadedQuads, dataGraphURI);
            }
        }
        return totalLoadedQuads;
    }

    private int addQuadsFromQuery(URI dataGraphURI, String sparqlQuery, RDFHandler rdfHandler) throws OpenRDFException, DataUnitException {
        int quadCount = 0;
        RepositoryConnection connection = getConnection();
        TupleQueryResult resultSet = connection.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery).evaluate();
        try {
            ValueFactory valueFactory = connection.getRepository().getValueFactory();
            while (resultSet.hasNext()) {
                BindingSet bindings = resultSet.next();
                Statement quad = valueFactory.createStatement(
                        (Resource) bindings.getValue("s"),
                        (URI) bindings.getValue("p"),
                        bindings.getValue("o"),
                        dataGraphURI);
                rdfHandler.handleStatement(quad);
                quadCount++;
            }
        } finally {
            resultSet.close();
            //if (connection instanceof VirtuosoRepositoryConnection)
            //    // fix potential error "Too many open statements" - Virtuoso doesn't release resources properly
            //    try {
            //        closeConnection();
            //    } catch (RepositoryException e) {
            //        // ignore
            //    }
            //}
        }
        return quadCount;
    }

    private RepositoryConnection getConnection() throws DataUnitException {
        if (_connection == null) {
            _connection = rdfInput.getConnection();
        }
        return _connection;
    }

    //private void closeConnection() throws RepositoryException {
    //    if (_connection != null) {
    //        try {
    //            _connection.close();
    //        } finally {
    //            _connection = null;
    //        }
    //    }
    //}

    private String formatQuery(URI graph, int limit, int offset) {
        return String.format(Locale.ROOT,
                LOAD_SPARQL_QUERY,
                graph.stringValue(),
                limit,
                offset);
    }

    @Override
    public URI getDefaultContext() throws LDFusionToolException {
        return defaultContext;
    }

    @Override
    public void close() throws LDFusionToolException {
        try {
            if (_connection != null) {
                _connection.close();
            }
        } catch (RepositoryException e) {
            throw new LDFusionToolException("Error closing data unit connection", e);
        }
    }
}
