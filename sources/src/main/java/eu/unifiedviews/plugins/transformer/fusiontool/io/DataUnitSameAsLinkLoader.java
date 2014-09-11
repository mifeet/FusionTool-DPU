package eu.unifiedviews.plugins.transformer.fusiontool.io;

import cz.cuni.mff.odcleanstore.fusiontool.config.LDFTConfigConstants;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterableImpl;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.util.CloseableRepositoryConnection;
import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.rdf.RDFDataUnit;
import eu.unifiedviews.plugins.transformer.fusiontool.FusionToolDpuExecutor;
import org.openrdf.OpenRDFException;
import org.openrdf.model.URI;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class DataUnitSameAsLinkLoader {
    private static final Logger LOG = LoggerFactory.getLogger(FusionToolDpuExecutor.class);
    private static RDFDataUnit dataUnit;
    private static Set<URI> sameAsLinkTypes;

    public DataUnitSameAsLinkLoader(RDFDataUnit dataUnit, Set<URI> sameAsLinkTypes) {
        this.dataUnit = dataUnit;
        this.sameAsLinkTypes = sameAsLinkTypes;
    }

    public void loadSameAsLinks(UriMappingIterableImpl uriMapping) throws ODCSFusionToolException {
        LOG.info("Loading sameAs links...");
        try (CloseableRepositoryConnection connection = new CloseableRepositoryConnection(dataUnit.getConnection())) {
            long startTime = System.currentTimeMillis();
            long loadedCount = loadFromConnection(uriMapping, connection.getConnection());
            LOG.info(String.format("Loaded & resolved %,d sameAs links in %,d ms", loadedCount, System.currentTimeMillis() - startTime));
        } catch (OpenRDFException | DataUnitException e) {
            throw new ODCSFusionToolException("Error when loading owl:sameAs links from input", e);
        }
    }

    private long loadFromConnection(UriMappingIterableImpl uriMapping, RepositoryConnection connection)
            throws QueryEvaluationException, RepositoryException, MalformedQueryException {

        long loadedCount = 0;
        for (URI link : sameAsLinkTypes) {
            String query = String.format("CONSTRUCT {?s <%1$s> ?o} WHERE {?s <%1$s> ?o}", link.stringValue());
            GraphQueryResult sameAsTriples = connection.prepareGraphQuery(QueryLanguage.SPARQL, query).evaluate();
            while (sameAsTriples.hasNext()) {
                uriMapping.addLink(sameAsTriples.next());
                loadedCount++;
                if (loadedCount % LDFTConfigConstants.LOG_LOOP_SIZE == 0) {
                    LOG.info("... loaded {} sameAs links", loadedCount);
                }
            }
        }
        return loadedCount;
    }
}
