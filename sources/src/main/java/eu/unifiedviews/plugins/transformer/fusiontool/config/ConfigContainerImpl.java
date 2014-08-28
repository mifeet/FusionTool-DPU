package eu.unifiedviews.plugins.transformer.fusiontool.config;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionStrategy;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolutionStrategyImpl;
import org.openrdf.model.URI;
import org.openrdf.rio.ParserConfig;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Container of configuration values.
 * @author Jan Michelfeit
 */
public class ConfigContainerImpl implements ConfigContainer {
    private String resultDataURIPrefix = FTConfigConstants.DEFAULT_RESULT_DATA_URI_PREFIX;
    private Map<String, String> prefixes = new HashMap<>();
    private ResolutionStrategy defaultResolutionStrategy = new ResolutionStrategyImpl();
    private Map<URI, ResolutionStrategy> propertyResolutionStrategies = new HashMap<>();

    private String canonicalURIsFileName = null;
    private List<FileOutput> fileOutputs = new LinkedList<>();
    private boolean enableFileCache = false;
    private Long maxOutputTriples = null;
    private boolean isProfilingOn = false;
    private boolean isLocalCopyProcessing = true;
    private Long memoryLimit = null;
    private URI requiredClassOfProcessedResources = null;
    private ParserConfig parserConfig = FTConfigConstants.DEFAULT_FILE_PARSER_CONFIG;

    private String seedResourceSparqlQuery;

    @Override
    public String getSeedResourceSparqlQuery() {
        return seedResourceSparqlQuery;
    }

    /**
     * Sets value for {@link #getSeedResourceSparqlQuery()}.
     * @param seedResourceSparqlQuery SPARQL query returning result set with a single bound variable
     */
    public void setSeedResourceSparqlQuery(String seedResourceSparqlQuery) {
        this.seedResourceSparqlQuery = seedResourceSparqlQuery;
    }

    @Override
    public String getResultDataURIPrefix() { // TODO: settable in XML configuration
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
    public boolean getOutputMappedSubjectsOnly() {
        return FTConfigConstants.OUTPUT_MAPPED_SUBJECTS_ONLY;
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
    public URI getRequiredClassOfProcessedResources() {
        return requiredClassOfProcessedResources;
    }

    /**
     * Sets value for {@link #getRequiredClassOfProcessedResources()}.
     * @param requiredClassOfProcessedResources see {@link #getRequiredClassOfProcessedResources()}
     */
    public void setRequiredClassOfProcessedResources(URI requiredClassOfProcessedResources) {
        this.requiredClassOfProcessedResources = requiredClassOfProcessedResources;
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
        this.canonicalURIsFileName = fileName;
    }

    @Override
    public ParserConfig getParserConfig() {
        return parserConfig;
    }

    /**
     * Sets value for {@link #getParserConfig()}.
     * @param parserConfig see {@link #getParserConfig()}
     */
    public void setParserConfig(ParserConfig parserConfig) {
        this.parserConfig = parserConfig;
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
    public boolean isLocalCopyProcessing() {
        return isLocalCopyProcessing;
    }

    /**
     * Sets value for {@link #isLocalCopyProcessing()}.
     * @param isLocalCopyProcessing see {@link #isLocalCopyProcessing()}
     */
    public void setLocalCopyProcessing(boolean isLocalCopyProcessing) {
        this.isLocalCopyProcessing = isLocalCopyProcessing;
    }

    @Override
    public Long getMemoryLimit() {
        return memoryLimit;
    }

    /**
     * Sets value for {@link #getMemoryLimit()}.
     * @param memoryLimit see {@link #getMemoryLimit()}
     */
    public void setMemoryLimit(Long memoryLimit) {
        this.memoryLimit = memoryLimit;
    }

    @Override
    public float getMaxFreeMemoryUsage() {
        return FTConfigConstants.MAX_FREE_MEMORY_USAGE;
    }

    @Override
    public Integer getQueryTimeout() {
        return FTConfigConstants.DEFAULT_QUERY_TIMEOUT;
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
    public Double getAgreeCoefficient() {
        return FTConfigConstants.AGREE_COEFFICIENT;
    }


    @Override
    public Double getScoreIfUnknown() {
        return FTConfigConstants.SCORE_IF_UNKNOWN;
    }

    @Override
    public Double getPublisherScoreWeight() {
        return FTConfigConstants.PUBLISHER_SCORE_WEIGHT;
    }

    @Override
    public Long getMaxDateDifference() {
        return FTConfigConstants.MAX_DATE_DIFFERENCE;
    }


    @Override
    public Collection<String> getPreferredCanonicalURIs() {
        return FTConfigConstants.DEFAULT_PREFERRED_CANONICAL_URIS;
    }

    @Override
    public Set<URI> getSameAsLinkTypes() {
        return FTConfigConstants.SAME_AS_LINK_TYPES;
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
    public Long getMaxOutputTriples() {
        return maxOutputTriples;
    }

    /**
     * Sets value for {@link #getMaxOutputTriples()}.
     */
    public void setMaxOutputTriples(Long maxOutputTriples) {
        this.maxOutputTriples = maxOutputTriples;
    }
}