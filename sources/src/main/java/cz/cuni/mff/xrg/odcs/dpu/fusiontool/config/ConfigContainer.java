package cz.cuni.mff.xrg.odcs.dpu.fusiontool.config;

import java.util.Collection;
import java.util.Map;

import org.openrdf.model.URI;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionStrategy;

/**
 * Encapsulation of ODCS-FusionTool configuration.
 * @author Jan Michelfeit
 */
public interface ConfigContainer {
    /**
     * Map of namespace prefixes that can be used (e.g. in SPARQL expressions or aggregation settings).
     * Key is the prefix, value the expanded URI.
     * @return map of namespace prefixes
     */
    Map<String, String> getPrefixes();
    
    /**
     * Prefix of named graphs and URIs where query results and metadata in the output are placed.
     * @return graph name prefix
     */
    String getResultDataURIPrefix();
    
    /**
     * Default conflict resolution strategy.
     * @return resolution strategy
     */
    ResolutionStrategy getDefaultResolutionStrategy();
    
    /**
     * Conflicts resolution strategy settings for individual properties.
     * Key is the property URI (must have expanded namespace prefix), value the actual strategy.
     * @return map of resolution strategies indexed by property URIs 
     */
    Map<URI, ResolutionStrategy> getPropertyResolutionStrategies();

    /**
     * Coefficient used in quality computation formula. Value N means that (N+1)
     * sources with score 1 that agree on the result will increase the result
     * quality to 1.
     * @return agree coefficient
     */
    Double getAgreeCoeficient();

    /**
     * Graph score used if none is given in the input.
     * @return default score
     */
    Double getScoreIfUnknown();

    /**
     * Weight of the publisher score.
     * @return publisher score weight
     */
    Double getPublisherScoreWeight();

    /**
     * Difference between two dates when their distance is equal to MAX_DISTANCE in seconds.
     * @return time interval in seconds
     */
    Long getMaxDateDifference();
    
    /**
     * Name of file where resolved canonical URIs are read from and written to. 
     * Null means that canonical URIs will not be written anywhere.
     * @return name of file with canonical URIs or null
     */
    String getCanonicalURIsFileName();
    
    /**
     * Indicates whether disable (true) or enable (false) file cache for objects that needed by CR algorithm
     * that may not fit into the memory.
     * @return whether to disable algorithm file cache
     */
    boolean getEnableFileCache();
    
    /**
     * SPARQL query returning URI resources which are initially loaded and processed.
     * The query must be a SELECT query binding a single variable in the result set.
     * If given, triples having matching resources and triples reachable from them are processed. All data
     * from matching input graphs are processed otherwise.
     * @return SPARQL query or null  
     */
    String getSeedResourceSparqlQuery();
    
    /**
     * Returns a default set of preferred URIs. 
     * These are added to preferred URIs obtained from configuration and canonical URI file.
     * @return set of preferred canonical URIs
     */
    Collection<String> getPreferredCanonicalURIs();
    
    /**
     * Returns true of profiling logs should be printed.
     * @return true iff profiling logs should be printed
     */
    boolean isProfilingOn();
}