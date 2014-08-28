/**
 * 
 */
package eu.unifiedviews.plugins.transformer.fusiontool.config.xml;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

import java.util.List;

// CHECKSTYLE:OFF

/**
 * @author Jan Michelfeit
 */
@Root(name = "FileOutputsXml")
public class FileOutputsXml {
    @Attribute(required = false)
    private Long maxResolvedQuads;
    
    @ElementList(name = "FileOutput", required = false, inline = true, empty = false)
    private List<FileOutputXml> fileOutputs;

    public Long getMaxResolvedQuads() {
        return maxResolvedQuads;
    }
    
    public List<FileOutputXml> getFileOutputs() {
        return fileOutputs;
    }
}
