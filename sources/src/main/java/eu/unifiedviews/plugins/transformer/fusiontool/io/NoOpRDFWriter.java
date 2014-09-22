package eu.unifiedviews.plugins.transformer.fusiontool.io;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriterBase;
import org.openrdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class NoOpRDFWriter extends CloseableRDFWriterBase {
    private static final Logger LOG = LoggerFactory.getLogger(NoOpRDFWriter.class);

    @Override
    public void write(Statement quad) throws IOException {
        LOG.trace("Writing to NoOpRDFWriter: {}", quad);
    }

    @Override
    public void write(ResolvedStatement resolvedStatement) throws IOException {
        LOG.trace("Writing to NoOpRDFWriter: {}", resolvedStatement);
    }

    @Override
    public void addNamespace(String prefix, String uri) throws IOException {
        // do nothing
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
