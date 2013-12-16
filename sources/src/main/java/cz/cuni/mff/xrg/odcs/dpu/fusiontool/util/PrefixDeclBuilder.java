package cz.cuni.mff.xrg.odcs.dpu.fusiontool.util;

/**
 * SPARQL prefix declarations holder. 
 * @author Jan Michelfeit
 */
public interface PrefixDeclBuilder {
    /**
     * Returns a SPARQL snippet with namespace prefix declarations.
     * @return SPARQL query snippet
     */
    String getPrefixDecl();
}
