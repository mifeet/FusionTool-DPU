/**
 * 
 */
package cz.cuni.mff.xrg.odcs.dpu.fusiontool.config.xml;

import java.util.List;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

// CHECKSTYLE:OFF

/**
 * @author Jan Michelfeit
 */
@Root(name = "FileOutputsXml")
public class FileOutputsXml {
    @Attribute(required = false)
    private Integer maxResolvedQuads;
    
    @ElementList(name = "FileOutput", required = false, inline = true, empty = false)
    private List<FileOutputXml> fileOutputs;

    public Integer getMaxResolvedQuads() {
        return maxResolvedQuads;
    }
    
    public List<FileOutputXml> getFileOutputs() {
        return fileOutputs;
    }
}
