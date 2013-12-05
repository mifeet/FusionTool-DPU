package cz.cuni.mff.xrg.odcs.dpu.fusiontool.config;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
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

    private File canonicalURIsOutputFile = null;
    private File canonicalURIsInputFile;
    private boolean enableFileCache = false;

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
    public File getCanonicalURIsOutputFile() {
        return canonicalURIsOutputFile;
    }

    /**
     * Sets file where resolved canonical URIs shall be written.
     * @param file file to write canonical URIs to
     */
    public void setCanonicalURIsOutputFile(File file) {
        this.canonicalURIsOutputFile = file;
    }

    @Override
    public File getCanonicalURIsInputFile() {
        return canonicalURIsInputFile;
    }

    /**
     * Sets file with list of preferred canonical URIs.
     * @param file file with canonical URIs
     */
    public void setCanonicalURIsInputFile(File file) {
        this.canonicalURIsInputFile = file;
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
    public Integer getQueryTimeout() {
        return ConfigConstants.DEFAULT_QUERY_TIMEOUT;
    }

    @Override
    public Double getAgreeCoeficient() {
        return ConfigConstants.AGREE_COEFFICIENT;
    }

    @Override
    public Double getScoreIfUnknown() {
        return ConfigConstants.SCORE_IF_UNKNOWN;
    }

    @Override
    public Double getPublisherScoreWeight() {
        return ConfigConstants.PUBLISHER_SCORE_WEIGHT;
    }

    @Override
    public Long getMaxDateDifference() {
        return ConfigConstants.MAX_DATE_DIFFERENCE;
    }

    @Override
    public Collection<String> getPreferredCanonicalURIs() {
        return ConfigConstants.DEFAULT_PREFERRED_CANONICAL_URIS;
    }
}
