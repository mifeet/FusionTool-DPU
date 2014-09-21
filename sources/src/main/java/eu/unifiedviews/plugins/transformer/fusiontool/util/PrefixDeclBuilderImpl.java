package eu.unifiedviews.plugins.transformer.fusiontool.util;

import java.util.Map;

/**
 * Utility class mapping namespace prefixes to SPARQL prefix declarations. 
 * @author Jan Michelfeit
 */
@Deprecated
public class PrefixDeclBuilderImpl implements PrefixDeclBuilder {
    private final String prefixDecl;
    
    /**
     * @param nsPrefixMap Map of namespace prefixes that can be used (key is the prefix, value the expanded URI).
     */
    public PrefixDeclBuilderImpl(Map<String, String> nsPrefixMap) {
        this.prefixDecl = buildPrefixDecl(nsPrefixMap);
    }
    
    /**
     * Returns a SPARQL snippet with namespace prefix declarations.
     * @return SPARQL query snippet
     */
    public String getPrefixDecl() {
        return prefixDecl;
    }

    /**
     * Creates SPARQL snippet with prefix declarations for the given namespace prefixes.
     * @param prefixes namespace prefixes
     * @return SPARQL query snippet
     */
    private static String buildPrefixDecl(Map<String, String> prefixes) {
        if (prefixes == null) {
            return "";
        }
        StringBuilder result = new StringBuilder("");
        for (Map.Entry<String, String> entry: prefixes.entrySet()) {
            result.append("\n PREFIX ")
                .append(entry.getKey())
                .append(": <")
                .append(entry.getValue())
                .append("> ");
        }
        return result.toString();
    }
}
