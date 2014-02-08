/**
 * 
 */
package cz.cuni.mff.xrg.odcs.dpu.fusiontool.config;

import java.io.File;

import org.openrdf.model.URI;

import cz.cuni.mff.xrg.odcs.dpu.fusiontool.io.EnumSerializationFormat;

/**
 * Container of settings for a file output of result data.
 * @author Jan Michelfeit
 */
public interface FileOutput {
    /**
     * Returns a human-readable name of this output.
     * @return human-readable name or null if unavailable
     */
    File getPath();
    
    /**
     * Returns output serialization format.
     * @return output format
     */
    EnumSerializationFormat getFormat();
    
    /**
     * Returns URI of named graph where resolved quads will be placed. 
     * Overrides the unique named graph assigned to each resolved quad from Conflict Resolution.
     * @return named graph URI
     */
    URI getDataContext();

    /**
     * Returns URI of named graph where Conflict Resolution metadata will be placed. 
     * @return named graph URI
     */
    URI getMetadataContext();
}
