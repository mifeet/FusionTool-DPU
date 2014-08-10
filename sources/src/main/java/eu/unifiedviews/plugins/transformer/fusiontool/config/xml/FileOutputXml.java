package eu.unifiedviews.plugins.transformer.fusiontool.config.xml;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.Root;

// CHECKSTYLE:OFF
/**
 * @author Jan Michelfeit
 */
@Root(name = "FileOutput")
public class FileOutputXml {
    @Attribute
    private String path;
    
    @Attribute
    private String format;
    
    @Attribute(required = false)
    private String dataContext;
    
    @Attribute(required = false)
    private String metadataContext;

    public String getPath() {
        return path;
    }
    
    public String getFormat() {
        return format;
    }
    
    public String getDataContext() {
        return dataContext;
    }
    
    public String getMetadataContext() {
        return metadataContext;
    }
}
