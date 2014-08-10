/**
 * 
 */
package eu.unifiedviews.plugins.transformer.fusiontool.config.xml;

import java.util.List;

import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

// CHECKSTYLE:OFF

/**
 * @author Jan Michelfeit
 */
@Root(name = "DataProcessing")
public class DataProcessingXml {
    @ElementList(name = "Params", required = false, empty = false, inline = true)
    private List<ParamXml> params;
    
    @Element(name = "SeedResourceSparqlQuery", required = false)
    private String seedResourceSparqlQuery;
    
    public List<ParamXml> getParams() {
        return params;
    }

    public String getSeedResourceSparqlQuery() {
        return seedResourceSparqlQuery;
    }

}
