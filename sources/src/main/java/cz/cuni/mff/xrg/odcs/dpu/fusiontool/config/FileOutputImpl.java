/**
 * 
 */
package cz.cuni.mff.xrg.odcs.dpu.fusiontool.config;

import java.io.File;

import org.openrdf.model.URI;

import cz.cuni.mff.xrg.odcs.dpu.fusiontool.io.EnumSerializationFormat;

/**
 * Container of settings for an file output of result data.
 * @author Jan Michelfeit
 */
public class FileOutputImpl implements FileOutput {
    private final File path;
    private final EnumSerializationFormat format;
    private URI metadataContext;
    private URI dataContext;
    
    /**
     * @param path output path
     * @param format file serialization format
     */
    public FileOutputImpl(File path, EnumSerializationFormat format) {
        this.path = path;
        this.format = format;
    }

    @Override
    public File getPath() {
        return path; 
    }
    
    @Override
    public EnumSerializationFormat getFormat() {
        return format; 
    }

    @Override
    public URI getMetadataContext() {
        return metadataContext;
    }
    
    /**
     * Sets value for {@link #getMetadataContext()}.
     * @param metadataContext named graph URI
     */
    public void setMetadataContext(URI metadataContext) {
        this.metadataContext = metadataContext;
    }
    
    @Override
    public URI getDataContext() {
        return dataContext;
    }
    
    /**
     * Sets value for {@link #getDataContext()}.
     * @param dataContext named graph URI
     */
    public void setDataContext(URI dataContext) {
        this.dataContext = dataContext;
    }
}
