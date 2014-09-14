package eu.unifiedviews.plugins.transformer.fusiontool.io;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriterBase;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;
import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.rdf.WritableRDFDataUnit;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class DataUnitRDFWriterWithMetadata extends CloseableRDFWriterBase {
    public static final String DATA_GRAPH_NAME_PREFIX = "result-";
    public static final String METADATA_GRAPH_NAME = "metadata";
    private final RepositoryConnection connection;
    private final URI defaultContext;
    private final WritableRDFDataUnit dataUnit;
    private final URI metadataContext;
    private final AtomicLong counter = new AtomicLong(0);
    private final ValueFactory valueFactory;

    public DataUnitRDFWriterWithMetadata(WritableRDFDataUnit dataUnit) throws DataUnitException {
        this.dataUnit = dataUnit;
        connection = dataUnit.getConnection();
        defaultContext = dataUnit.getBaseDataGraphURI();
        metadataContext = dataUnit.addNewDataGraph(METADATA_GRAPH_NAME);
        valueFactory = connection.getValueFactory();
    }

    @Override
    public void write(Statement quad) throws IOException {
        try {
            connection.add(quad, defaultContext);
        } catch (RepositoryException e) {
            throw new IOException("Error writing to data unit", e);
        }
    }

    @Override
    public void write(ResolvedStatement resolvedStatement) throws IOException {
        try {
            URI statementContext = dataUnit.addNewDataGraph(DATA_GRAPH_NAME_PREFIX + Long.toString(counter.incrementAndGet()));
            connection.add(resolvedStatement.getStatement(), statementContext);
            connection.add(statementContext, ODCS.QUALITY, valueFactory.createLiteral(resolvedStatement.getQuality()), metadataContext);
            for (Resource sourceGraph : resolvedStatement.getSourceGraphNames()) {
                connection.add(statementContext, ODCS.SOURCE_GRAPH, sourceGraph, metadataContext);
            }
        } catch (DataUnitException | RepositoryException e) {
            throw new IOException("Error writing to data unit", e);
        }
    }

    @Override
    public void addNamespace(String prefix, String uri) throws IOException {
        /* do nothing */
    }

    @Override
    public void close() throws IOException {
        try {
            connection.close();
        } catch (RepositoryException e) {
            throw new IOException("Error closing data unit connection", e);
        }
    }
}
