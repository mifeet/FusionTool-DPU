package eu.unifiedviews.plugins.transformer.fusiontool.io;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriterBase;
import org.openrdf.model.Statement;

import java.io.IOException;

public class NoOpRDFWriter extends CloseableRDFWriterBase {
    @Override
    public void write(Statement quad) throws IOException {
        // do nothing
    }

    @Override
    public void write(ResolvedStatement resolvedStatement) throws IOException {
        // do nothing
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
