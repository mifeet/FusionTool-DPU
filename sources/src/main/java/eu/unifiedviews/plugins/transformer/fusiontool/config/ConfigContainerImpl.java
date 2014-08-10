package eu.unifiedviews.plugins.transformer.fusiontool.config;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.openrdf.model.URI;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionStrategy;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolutionStrategyImpl;

/**
 * Container of configuration values.
 * @author Jan Michelfeit
 */
public class ConfigContainerImpl implements ConfigContainer {
    private String seedResourceSparqlQuery;
    private String resultDataURIPrefix = ConfigConstants.DEFAULT_RESULT_DATA_URI_PREFIX;
    private Map<String, String> prefixes = new HashMap<String, String>();
    private ResolutionStrategy defaultResolutionStrategy = new ResolutionStrategyImpl();
    private Map<URI, ResolutionStrategy> propertyResolutionStrategies = new HashMap<URI, ResolutionStrategy>();
    private List<FileOutput> fileOutputs = new LinkedList<FileOutput>();

    private boolean enableFileCache = false;
    private boolean isProfilingOn = false;
    private String canonicalURIsFileName = null;
    private Integer fileOutputMaxResolvedQuads = null;

    @Override
    public String getSeedResourceSparqlQuery() {
        return seedResourceSparqlQuery;
    }

    /**
     * Sets value for {@link #getSeedResourceRestriction()}.
     * @param seedResourceSparqlQuery SPARQL query returning result set with a single bound variable
     */
    public void setSeedResourceSparqlQuery(String seedResourceSparqlQuery) {
        this.seedResourceSparqlQuery = seedResourceSparqlQuery;
    }

    @Override
    public String getResultDataURIPrefix() {
        return resultDataURIPrefix;
    }

    /**
     * Sets prefix of named graphs and URIs where query results and metadata in the output are placed.
     * @param resultDataURIPrefix named graph URI prefix
     */
    public void setResultDataURIPrefix(String resultDataURIPrefix) {
        this.resultDataURIPrefix = resultDataURIPrefix;
    }

    @Override
    public Map<String, String> getPrefixes() {
        return prefixes;
    }

    /**
     * Sets map of defined namespace prefixes.
     * @param prefixes map of namespace prefixes
     */
    public void setPrefixes(Map<String, String> prefixes) {
        this.prefixes = prefixes;
    }

    @Override
    public ResolutionStrategy getDefaultResolutionStrategy() {
        return defaultResolutionStrategy;
    }

    /**
     * Setter for value of {@link #getDefaultResolutionStrategy()}.
     * @param strategy conflict resolution strategy
     */
    public void setDefaultResolutionStrategy(ResolutionStrategy strategy) {
        this.defaultResolutionStrategy = strategy;
    }

    @Override
    public Map<URI, ResolutionStrategy> getPropertyResolutionStrategies() {
        return propertyResolutionStrategies;
    }

    /**
     * Setter for value of {@link #getDefaultResolutionStrategy()}.
     * @param strategies per-property conflict resolution strategies
     */
    public void setPropertyResolutionStrategies(Map<URI, ResolutionStrategy> strategies) {
        this.propertyResolutionStrategies = strategies;
    }
    
    @Override
    public String getCanonicalURIsFileName() {
        return canonicalURIsFileName;
    }
    
    /**
     * Setter for value {@link #getCanonicalURIsFileName()}.
     * @param fileName name of file with canonical URIs
     */
    public void setCanonicalURIsFileName(String fileName) {
        this.canonicalURIsFileName  = fileName;
    }

    @Override
    public boolean getEnableFileCache() {
        return enableFileCache;
    }

    /**
     * Sets value for {@link #getEnableFileCache()}.
     * @param enableFileCache see {@link #getEnableFileCache()}
     */
    public void setEnableFileCache(boolean enableFileCache) {
        this.enableFileCache = enableFileCache;
    }
    
    @Override
    public List<FileOutput> getFileOutputs() {
        return fileOutputs;
    }
    
    /**
     * Sets result outputs.
     * @param fileOutputs list of file outputs 
     */
    public void setFileOutputs(List<FileOutput> fileOutputs) {
        this.fileOutputs = fileOutputs;
    }

    @Override
    public Double getAgreeCoeficient() {
        return ConfigConstants.AGREE_COEFFICIENT;
    }

    @Override
    public Collection<String> getPreferredCanonicalURIs() {
        return ConfigConstants.DEFAULT_PREFERRED_CANONICAL_URIS;
    }
    
    @Override
    public int getTriplesChunkSize() {
        return ConfigConstants.TRIPLES_CHUNK_SIZE;
    }
    
    @Override
    public boolean isProfilingOn() {
        return isProfilingOn; 
    }
    
    /**
     * Sets value for {@link #isProfilingOn()}.
     * @param isProfilingOn see {@link #isProfilingOn()}
     */
    public void setProfilingOn(boolean isProfilingOn) {
        this.isProfilingOn = isProfilingOn;
    }

    @Override
    public Integer getFileOutputMaxResolvedQUads() {
        return fileOutputMaxResolvedQuads;
    }
    
    /**
     * Sets value for {@link #getFileOutputMaxResolvedQUads()}.
     * @param fileOutputMaxResolvedQuads see {@link #getFileOutputMaxResolvedQUads()}
     */
    public void setFileOutputMaxResolvedQuads(Integer fileOutputMaxResolvedQuads) {
        this.fileOutputMaxResolvedQuads  = fileOutputMaxResolvedQuads;
    }
}
