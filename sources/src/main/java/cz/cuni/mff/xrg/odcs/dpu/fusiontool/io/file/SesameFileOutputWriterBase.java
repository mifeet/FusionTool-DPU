/**
 * 
 */
package cz.cuni.mff.xrg.odcs.dpu.fusiontool.io.file;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;


/**
 * Implementation of {@link FileOutputWriter} writing to a given Sesame {@link RDFHandler}.
 * @author Jan Michelfeit
 */
public abstract class SesameFileOutputWriterBase implements FileOutputWriter {
    private final RDFHandler rdfWriter;
    private final Closeable underlyingResource;
    
    /**
     * Create a new instance.
     * @param rdfWriter handler or written RDF data
     * @param underlyingResource a resource to be closed as soon as writing is finished
     * @throws IOException  I/O error
     */
    protected SesameFileOutputWriterBase(RDFHandler rdfWriter, Closeable underlyingResource) throws IOException {
        this.rdfWriter = rdfWriter;
        this.underlyingResource = underlyingResource;
        try {
            this.rdfWriter.startRDF();
        } catch (RDFHandlerException e) {
            throw new IOException(e);
        }
    }
    
    @Override 
    public void addNamespace(String prefix, String uri) throws IOException {
        try {
            rdfWriter.handleNamespace(prefix, uri);
        } catch (RDFHandlerException e) {
            throw new IOException(e);
        }
    }
    
    @Override
    public final void writeResolvedStatements(Iterator<ResolvedStatement> resolvedStatements) throws IOException {
        while (resolvedStatements.hasNext()) {
            write(resolvedStatements.next());
        } 
    }
    
    @Override
    public final void writeQuads(Iterator<Statement> quads) throws IOException {
        while (quads.hasNext()) {
            write(quads.next());
        } 
    }

    @Override
    public final void write(Statement statement) throws IOException {
        try {
            rdfWriter.handleStatement(statement);
        } catch (RDFHandlerException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            rdfWriter.endRDF();
        } catch (RDFHandlerException e) {
            throw new IOException(e);
        }
        underlyingResource.close();
    }
}
