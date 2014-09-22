package eu.unifiedviews.plugins.transformer.fusiontool.io;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriterBase;
import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.rdf.WritableRDFDataUnit;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import java.io.IOException;

public class DataUnitRDFWriter extends CloseableRDFWriterBase {
    private final RepositoryConnection connection;
    private final URI context;

    public DataUnitRDFWriter(WritableRDFDataUnit dataUnit, String dataGraphSymbolicName) throws DataUnitException {
        this.connection = dataUnit.getConnection();
        context = dataUnit.addNewDataGraph(dataGraphSymbolicName);
    }

    @Override
    public void write(Statement quad) throws IOException {
        try {
            connection.add(quad, context);
        } catch (RepositoryException e) {
            throw new IOException("Error writing to data unit", e);
        }
    }

    @Override
    public void write(ResolvedStatement resolvedStatement) throws IOException {
        write(resolvedStatement.getStatement());
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
