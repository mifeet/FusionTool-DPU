package eu.unifiedviews.plugins.transformer.fusiontool;

import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolverFactory;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionFunctionRegistry;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ConflictResolutionPolicyImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.DistanceMeasureImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.EmptyMetadataModel;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.SourceQualityCalculator;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.impl.DecidingConflictFQualityCalculator;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.impl.ODCSSourceQualityCalculator;
import cz.cuni.mff.odcleanstore.fusiontool.FusionComponentFactory;
import cz.cuni.mff.odcleanstore.fusiontool.FusionExecutor;
import cz.cuni.mff.odcleanstore.fusiontool.LDFusionToolExecutor;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl.NestedResourceDescriptionQualityCalculatorImpl;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl.ResourceDescriptionConflictResolverImpl;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.AlternativeUriNavigator;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterableImpl;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.ExternalSortingInputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.InputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.data.AllTriplesLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter.FederatedResourceDescriptionFilter;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter.MappedResourceFilter;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter.RequiredClassFilter;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter.ResourceDescriptionFilter;
import cz.cuni.mff.odcleanstore.fusiontool.util.*;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriter;
import cz.cuni.mff.odcleanstore.fusiontool.writers.NoOpUriMappingWriter;
import cz.cuni.mff.odcleanstore.fusiontool.writers.UriMappingWriter;
import cz.cuni.mff.odcleanstore.vocabulary.ODCSInternal;
import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.rdf.RDFDataUnit;
import eu.unifiedviews.dataunit.rdf.WritableRDFDataUnit;
import eu.unifiedviews.dpu.DPUContext;
import eu.unifiedviews.plugins.transformer.fusiontool.config.ConfigContainer;
import eu.unifiedviews.plugins.transformer.fusiontool.config.FTConfigConstants;
import eu.unifiedviews.plugins.transformer.fusiontool.io.AllTriplesDataUnitLoader;
import eu.unifiedviews.plugins.transformer.fusiontool.io.DataUnitRDFWriter;
import eu.unifiedviews.plugins.transformer.fusiontool.io.DataUnitRDFWriterWithMetadata;
import eu.unifiedviews.plugins.transformer.fusiontool.io.DataUnitSameAsLinkLoader;
import eu.unifiedviews.plugins.transformer.fusiontool.io.NoOpRDFWriter;
import org.openrdf.model.Model;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.TreeModel;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Fuses RDF data from input using ODCS Conflict Resolution and writes the output to RDF outputs.
 * Conflict resolution includes resolution of owl:sameAs link, resolution of instance-level conflicts.
 * See sample configuration files (sample-config-full.xml) for overview of all processing options.
 * @author Jan Michelfeit
 */
public class FusionToolDpuComponentFactory implements FusionComponentFactory {
    //private static final Logger LOG = LoggerFactory.getLogger(FusionToolDpuComponentFactory.class);

    private ConfigContainer config;
    private DPUContext executionContext;
    private List<? extends RDFDataUnit> rdfInputs;
    private RDFDataUnit sameAsInput;
    private RDFDataUnit metadataInput;
    private WritableRDFDataUnit rdfOutput;
    private ProfilingTimeCounter<EnumFusionCounters> executorTimeProfiler;
    private MemoryProfiler executorMemoryProfiler;

    /**
     * Creates a new instance.
     * @param config configuration
     * @param executionContext execution context
     * @param rdfInputs RDF input data
     * @param sameAsInput input owl:sameAs links
     * @param metadataInput input metadata
     * @param rdfOutput RDF output data
     */
    public FusionToolDpuComponentFactory(
            ConfigContainer config, DPUContext executionContext, List<? extends RDFDataUnit> rdfInputs,
            RDFDataUnit sameAsInput, RDFDataUnit metadataInput, WritableRDFDataUnit rdfOutput) {
        this.config = config;
        this.executionContext = executionContext;
        this.rdfInputs = rdfInputs;
        this.sameAsInput = sameAsInput;
        this.metadataInput = metadataInput;
        this.rdfOutput = rdfOutput;
        this.executorTimeProfiler = ProfilingTimeCounter.createInstance(EnumFusionCounters.class, config.isProfilingOn());
        this.executorMemoryProfiler = MemoryProfiler.createInstance(config.isProfilingOn());
    }

    @Override
    public FusionExecutor getExecutor(UriMappingIterable uriMapping) {
        LDFusionToolExecutor executor = new LDFusionToolExecutor(
                true,
                config.getMaxOutputTriples(),
                getInputFilter(uriMapping),
                executorTimeProfiler,
                executorMemoryProfiler
        );
        executor.setIsCanceledCallback(new DPUContextIsCanceledCallback(executionContext));
        return executor;
    }

    @Override
    public InputLoader getInputLoader() throws IOException, LDFusionToolException {
        if (!config.isLocalCopyProcessing()) {
            throw new IllegalStateException("Non-local copy processing is not supported in DPU");
        }
        long memoryLimit = calculateMemoryLimit();
        Collection<AllTriplesLoader> allTriplesLoaders = getAllTriplesLoaders(rdfInputs);
        return new ExternalSortingInputLoader(
                allTriplesLoaders,
                LDFusionToolUtils.getResourceDescriptionProperties(config),
                executionContext.getWorkingDir(),
                config.getParserConfig(),
                memoryLimit);
    }

    protected ResourceDescriptionFilter getInputFilter(UriMappingIterable uriMapping) {
        List<ResourceDescriptionFilter> inputFilters = new ArrayList<>();
        if (config.getRequiredClassOfProcessedResources() != null) {
            inputFilters.add(new RequiredClassFilter(uriMapping, config.getRequiredClassOfProcessedResources()));
        }
        if (config.getOutputMappedSubjectsOnly()) {
            inputFilters.add(new MappedResourceFilter(new AlternativeUriNavigator(uriMapping)));
        }

        return FederatedResourceDescriptionFilter.fromList(inputFilters);
    }

    protected Collection<AllTriplesLoader> getAllTriplesLoaders(List<? extends RDFDataUnit> rdfInputs) throws LDFusionToolException {
        List<AllTriplesLoader> loaders = new ArrayList<>(rdfInputs.size());
        for (RDFDataUnit rdfInput : rdfInputs) {
            try {
                AllTriplesDataUnitLoader loader = new AllTriplesDataUnitLoader(rdfInput);
                loaders.add(loader);
            } catch (DataUnitException e) {
                // clean up already initialized loaders
                for (AllTriplesLoader loader : loaders) {
                    LDFusionToolUtils.closeQuietly(loader);
                }
                throw new LDFusionToolException("Error creating triple loader from RDF data unit: " + e.getMessage(), e);
            }
        }
        return loaders;
    }

    @Override
    public Model getMetadata() throws LDFusionToolException {
        if (metadataInput == null) {
            return new EmptyMetadataModel();
        }
        Model metadata = new TreeModel();
        RepositoryResult<Statement> metadataResult;
        try (CloseableRepositoryConnection connection = new CloseableRepositoryConnection(metadataInput.getConnection())) {
            metadataResult = connection.get().getStatements(null, null, null, false);
            while (metadataResult.hasNext()) {
                metadata.add(metadataResult.next());
            }
            return metadata;
        } catch (RepositoryException | DataUnitException e) {
            throw new LDFusionToolException("Error when loading metadata from input", e);
        }
    }

    @Override
    public UriMappingIterable getUriMapping() throws LDFusionToolException, IOException {
        // FIXME: preference of prefixes from configuration
        Set<String> preferredURIs = getPreferredURIs();
        UriMappingIterableImpl uriMapping = new UriMappingIterableImpl(preferredURIs);
        if (sameAsInput != null) {
            DataUnitSameAsLinkLoader sameAsLoader = new DataUnitSameAsLinkLoader(sameAsInput, config.getSameAsLinkTypes());
            sameAsLoader.loadSameAsLinks(uriMapping);
        }
        return uriMapping;
    }

    /**
     * Returns set of URIs preferred for canonical URIs.
     * The URIs are loaded from canonicalURIsInputFile if given and URIs present in settingsPreferredURIs are added.
     * @return set of URIs preferred for canonical URIs
     * @throws java.io.IOException error reading canonical URIs from file
     */
    protected Set<String> getPreferredURIs() throws IOException {
        Collection<String> preferredCanonicalURIs = config.getPreferredCanonicalURIs();
        File canonicalURIsInputFile = getCanonicalUrisFile();
        Set<URI> settingsPreferredURIs = config.getPropertyResolutionStrategies().keySet();

        Set<String> preferredURIs = new HashSet<>(settingsPreferredURIs.size());
        for (URI uri : settingsPreferredURIs) {
            preferredURIs.add(uri.stringValue());
        }
        if (canonicalURIsInputFile != null) {
            new CanonicalUriFileHelper().readCanonicalUris(canonicalURIsInputFile, preferredURIs);
        }
        preferredURIs.addAll(preferredCanonicalURIs);

        return preferredURIs;
    }

    @Override
    public CloseableRDFWriter getRDFWriter() throws IOException, LDFusionToolException {
        try {
            if (rdfOutput == null) {
                return new NoOpRDFWriter();
            } else if (config.getWriteMetadata()) {
                return new DataUnitRDFWriterWithMetadata(rdfOutput);
            } else {
                return new DataUnitRDFWriter(rdfOutput);
            }
        } catch (DataUnitException e) {
            throw new LDFusionToolException("Error creating output writer", e);
        }
    }

    @Override
    public ResourceDescriptionConflictResolver getConflictResolver(Model metadata, UriMappingIterable uriMapping) {
        DistanceMeasureImpl distanceMeasure = new DistanceMeasureImpl();
        SourceQualityCalculator sourceQualityCalculator = new ODCSSourceQualityCalculator(
                config.getScoreIfUnknown(),
                config.getPublisherScoreWeight());
        ResolutionFunctionRegistry registry = ConflictResolverFactory.createInitializedResolutionFunctionRegistry(
                sourceQualityCalculator,
                config.getAgreeCoefficient(),
                distanceMeasure);
        NestedResourceDescriptionQualityCalculatorImpl nestedResourceDescriptionQualityCalculator = new NestedResourceDescriptionQualityCalculatorImpl(
                new DecidingConflictFQualityCalculator(sourceQualityCalculator, config.getAgreeCoefficient(), distanceMeasure));

        return new ResourceDescriptionConflictResolverImpl(
                registry,
                new ConflictResolutionPolicyImpl(config.getDefaultResolutionStrategy(), config.getPropertyResolutionStrategies()),
                uriMapping,
                metadata,
                config.getResultDataURIPrefix() + ODCSInternal.QUERY_RESULT_GRAPH_URI_INFIX + "/",
                nestedResourceDescriptionQualityCalculator
        );
    }

    @Override
    public UriMappingWriter getSameAsLinksWriter() throws IOException {
        return new NoOpUriMappingWriter();
    }

    @Override
    public UriMappingWriter getCanonicalUriWriter(UriMappingIterable uriMapping) throws IOException {
        if (config.getCanonicalURIsFileName() != null) {
            Set<String> canonicalUris = new HashSet<>();
            for (String mappedUri : uriMapping) {
                canonicalUris.add(uriMapping.getCanonicalURI(mappedUri));
            }
            new CanonicalUriFileHelper().writeCanonicalUris(getCanonicalUrisFile(), canonicalUris);
        }
        return new NoOpUriMappingWriter();
    }

    public ProfilingTimeCounter<EnumFusionCounters> getExecutorTimeProfiler() {
        return executorTimeProfiler;
    }

    private File getCanonicalUrisFile() {
        return new File(executionContext.getResultDir(), config.getCanonicalURIsFileName());
    }

    private long calculateMemoryLimit() {
        return config.getMemoryLimit() != null
                ? config.getMemoryLimit()
                : FTConfigConstants.MAX_MEMORY_LIMIT;
    }
}
