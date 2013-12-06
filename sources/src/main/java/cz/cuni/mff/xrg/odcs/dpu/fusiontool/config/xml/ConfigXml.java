/**
 * 
 */
package cz.cuni.mff.xrg.odcs.dpu.fusiontool.config.xml;

import java.util.List;

import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

// CHECKSTYLE:OFF

/**
 * @author Jan Michelfeit
 */
@Root(name = "Config")
public class ConfigXml {

    @ElementList(name = "Prefixes", required = false)
    private List<PrefixXml> prefixes;

    @Element(name="DataProcessing", required = false)
    private DataProcessingXml dataProcessing;

    @Element(name = "ConflictResolution", required = false)
    private ConflictResolutionXml conflictResolution;
    
    @Element(name = "Output", required =false)
    private OutputXml output;

    public List<PrefixXml> getPrefixes() {
        return prefixes;
    }

    public DataProcessingXml getDataProcessing() {
        return dataProcessing;
    }

    public ConflictResolutionXml getConflictResolution() {
        return conflictResolution;
    }
    
    public OutputXml getOutput() {
        return output;
    }
}
