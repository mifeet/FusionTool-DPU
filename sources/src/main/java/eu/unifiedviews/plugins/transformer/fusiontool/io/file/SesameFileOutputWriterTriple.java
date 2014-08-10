/**
 * 
 */
package eu.unifiedviews.plugins.transformer.fusiontool.io.file;

import java.io.Closeable;
import java.io.IOException;

import org.openrdf.rio.RDFHandler;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;

/**
 * Implementation of {@link FileOutputWriter} writing to a given Sesame {@link RDFHandler} writing only triples
 * regardless of named graphs.
 * @author Jan Michelfeit
 */
public class SesameFileOutputWriterTriple extends SesameFileOutputWriterBase {
    /**
     * @param rdfWriter handler or written RDF data
     * @param underlyingResource a resource to be closed as soon as writing is finished
     * @throws IOException I/O error
     */
    public SesameFileOutputWriterTriple(RDFHandler rdfWriter, Closeable underlyingResource) throws IOException {
        super(rdfWriter, underlyingResource);
    }

    @Override
    public void write(ResolvedStatement resolvedStatement) throws IOException {
        write(resolvedStatement.getStatement());
    }
}
