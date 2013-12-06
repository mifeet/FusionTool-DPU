/**
 * 
 */
package cz.cuni.mff.xrg.odcs.dpu.fusiontool.config.xml;

import java.util.List;

import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

// CHECKSTYLE:OFF

/**
 * @author Jan Michelfeit
 */
@Root(name = "Output")
public class OutputXml {
    @ElementList(name = "Params", required = false, empty = false, inline = true)
    private List<ParamXml> params;
    
    public List<ParamXml> getParams() {
        return params;
    }
}
