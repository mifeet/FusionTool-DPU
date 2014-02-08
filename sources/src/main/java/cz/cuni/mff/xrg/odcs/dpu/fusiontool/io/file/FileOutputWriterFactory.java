/**
 * 
 */
package cz.cuni.mff.xrg.odcs.dpu.fusiontool.io.file;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.openrdf.model.URI;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.n3.N3WriterFactory;
import org.openrdf.rio.rdfxml.util.RDFXMLPrettyWriterFactory;
import org.openrdf.rio.trig.TriGWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.xrg.odcs.dpu.fusiontool.config.FileOutput;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.exceptions.FusionToolDpuErrorCodes;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.exceptions.FusionToolDpuException;

/**
 * Factory class for {@link FileOutputWriter} instances.
 * @author Jan Michelfeit
 */
public class FileOutputWriterFactory {
    private static final Logger LOG = LoggerFactory.getLogger(FileOutputWriterFactory.class);

    /**
     * Creates a new {@link FileOutputWriter} according to settings given in output configuration.
     * @param output output configuration
     * @param baseDirectory base directory for relative output paths
     * @return RDF writer
     * @throws IOException I/O error
     * @throws FusionToolDpuException invalid output configuration
     */
    @SuppressWarnings("resource")
    public FileOutputWriter createRDFWriter(FileOutput output, File baseDirectory) throws IOException, FusionToolDpuException {
        URI metadataContext = output.getMetadataContext();
        URI dataContext = output.getDataContext();
        if (dataContext != null) {
            metadataContext = null; // data and metadata context are exclude each other
        }

        if (output.getPath() == null) {
            throw new FusionToolDpuException(FusionToolDpuErrorCodes.FILE_OUTPUT_PARAM, "File output path must be specified.");
        }
        if (output.getFormat() == null) {
            throw new FusionToolDpuException(FusionToolDpuErrorCodes.FILE_OUTPUT_PARAM, 
                    "File output serialization format must be specified.");
        }
        
        // Create output directory
        File path = output.getPath().isAbsolute() 
                ? output.getPath() 
                : new File(baseDirectory, output.getPath().getPath());
        File parent = path.getAbsoluteFile().getParentFile();
        if (parent == null || (!parent.exists() && !parent.mkdir())) {
            throw new FusionToolDpuException(FusionToolDpuErrorCodes.FILE_OUTPUT_DIRECTORY, 
                    "Unable to create directory for output files.");
        }
        
        Writer outputWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path), "UTF-8"));
        switch (output.getFormat()) {
        case RDF_XML:
            RDFHandler rdfXmlWriter = new RDFXMLPrettyWriterFactory().getWriter(outputWriter);
            LOG.info("Created RDF/XML output file '{}'", output.getPath().getPath());
            // RDFHandler rdfXmlWriter = new RDFXMLWriterFactory().getWriter(outputWriter);
            return new SesameFileOutputWriterTriple(rdfXmlWriter, outputWriter);
        case N3:
            RDFHandler n3Writer = new N3WriterFactory().getWriter(outputWriter);
            LOG.info("Created N3 output file '{}'", output.getPath().getPath());
            return new SesameFileOutputWriterTriple(n3Writer, outputWriter);
        case TRIG:
            RDFHandler trigHandler = new TriGWriterFactory().getWriter(outputWriter);
            LOG.info("Created TriG output file '{}'", output.getPath().getPath());
            return new SesameFileOutputWriterQuad(trigHandler, outputWriter, dataContext, metadataContext);
        case HTML:
            LOG.info("Created HTML output file '{}'", output.getPath().getPath());
            return new HtmlFileOutputWriter(outputWriter);
        default:
            throw new IllegalArgumentException("Unknown output format " + output.getFormat());
        }
    }
}
