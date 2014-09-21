package eu.unifiedviews.plugins.transformer.fusiontool.util;

/**
 * SPARQL prefix declarations holder. 
 * @author Jan Michelfeit
 */
@Deprecated
public interface PrefixDeclBuilder {
    /**
     * Returns a SPARQL snippet with namespace prefix declarations.
     * @return SPARQL query snippet
     */
    String getPrefixDecl();
}
