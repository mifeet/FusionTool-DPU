/**
 * 
 */
package eu.unifiedviews.plugins.transformer.fusiontool.config.xml;

import cz.cuni.mff.odcleanstore.fusiontool.config.xml.ConflictResolutionXml;
import cz.cuni.mff.odcleanstore.fusiontool.config.xml.PrefixXml;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

import java.util.List;

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
    
    public List<PrefixXml> getPrefixes() {
        return prefixes;
    }

    public DataProcessingXml getDataProcessing() {
        return dataProcessing;
    }

    public ConflictResolutionXml getConflictResolution() {
        return conflictResolution;
    }
}
