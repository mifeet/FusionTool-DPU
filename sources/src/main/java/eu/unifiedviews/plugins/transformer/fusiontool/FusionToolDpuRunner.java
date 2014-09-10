package eu.unifiedviews.plugins.transformer.fusiontool;

import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolverFactory;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionFunctionRegistry;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ConflictResolutionPolicyImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.DistanceMeasureImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.SourceQualityCalculator;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.impl.DecidingConflictFQualityCalculator;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.impl.ODCSSourceQualityCalculator;
import cz.cuni.mff.odcleanstore.fusiontool.AbstractFusionToolRunner;
import cz.cuni.mff.odcleanstore.fusiontool.ODCSFusionToolExecutor;
import cz.cuni.mff.odcleanstore.fusiontool.config.LDFTConfigConstants;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl.NestedResourceDescriptionQualityCalculatorImpl;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl.ResourceDescriptionConflictResolverImpl;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.AlternativeUriNavigator;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterableImpl;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.ExternalSortingInputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.InputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.data.AllTriplesLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter.FederatedResourceDescriptionFilter;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter.MappedResourceFilter;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter.RequiredClassFilter;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter.ResourceDescriptionFilter;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolAppUtils;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriter;
import cz.cuni.mff.odcleanstore.vocabulary.ODCSInternal;
import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.rdf.RDFDataUnit;
import eu.unifiedviews.dataunit.rdf.WritableRDFDataUnit;
import eu.unifiedviews.dpu.DPUContext;
import eu.unifiedviews.plugins.transformer.fusiontool.config.ConfigContainer;
import eu.unifiedviews.plugins.transformer.fusiontool.config.FTConfigConstants;
import eu.unifiedviews.plugins.transformer.fusiontool.io.AllTriplesDataUnitLoader;
import eu.unifiedviews.plugins.transformer.fusiontool.io.file.FileOutputWriterFactory;
import org.openrdf.OpenRDFException;
import org.openrdf.model.Model;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.TreeModel;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Fuses RDF data from input using ODCS Conflict Resolution and writes the output to RDF outputs.
 * Conflict resolution includes resolution of owl:sameAs link, resolution of instance-level conflicts.
 * See sample configuration files (sample-config-full.xml) for overview of all processing options.
 * @author Jan Michelfeit
 */
public class FusionToolDpuRunner extends AbstractFusionToolRunner {
    private static final Logger LOG = LoggerFactory.getLogger(FusionToolDpuExecutor.class);

    /** An instance of {@link eu.unifiedviews.plugins.transformer.fusiontool.io.file.FileOutputWriterFactory}. */
    protected static final FileOutputWriterFactory RDF_WRITER_FACTORY = new FileOutputWriterFactory();

    private ConfigContainer config;
    private DPUContext executionContext;
    private List<RDFDataUnit> rdfInputs;
    private RDFDataUnit sameAsInput;
    private RDFDataUnit metadataInput;
    private WritableRDFDataUnit rdfOutput;
    private boolean hasFusionToolRan;
    private int fileOutputWrittenQuads;

    /**
     * Creates a new instance.
     * @param config configuration
     * @param executionContext execution context
     * @param rdfInputs RDF input data
     * @param sameAsInput input owl:sameAs links
     * @param metadataInput input metadata
     * @param rdfOutput RDF output data
     */
    public FusionToolDpuRunner(
            ConfigContainer config, DPUContext executionContext, List<RDFDataUnit> rdfInputs,
            RDFDataUnit sameAsInput, RDFDataUnit metadataInput, WritableRDFDataUnit rdfOutput) {
        super(config.isProfilingOn());
        this.config = config;
        this.executionContext = executionContext;
        this.rdfInputs = rdfInputs;
        this.sameAsInput = sameAsInput;
        this.metadataInput = metadataInput;
        this.rdfOutput = rdfOutput;
        this.hasFusionToolRan = false;
        this.fileOutputWrittenQuads = 0;
    }

    @Override
    protected ODCSFusionToolExecutor createExecutor(UriMappingIterable uriMapping) {
        return new ODCSFusionToolExecutor(
                true,
                config.getMaxOutputTriples(),
                config.isProfilingOn(),
                getInputFilter(uriMapping));
    }

    @Override
    protected InputLoader getInputLoader() throws IOException, ODCSFusionToolException {
        if (!config.isLocalCopyProcessing()) {
            throw new IllegalStateException("Non-local copy processing is not supported in DPU");
        }
        long memoryLimit = calculateMemoryLimit();
        Collection<AllTriplesLoader> allTriplesLoaders = getAllTriplesLoaders(rdfInputs);
        return new ExternalSortingInputLoader(
                allTriplesLoaders,
                ODCSFusionToolAppUtils.getResourceDescriptionProperties(config),
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

    protected Collection<AllTriplesLoader> getAllTriplesLoaders(List<RDFDataUnit> rdfInputs) throws ODCSFusionToolException {
        List<AllTriplesLoader> loaders = new ArrayList<>(rdfInputs.size());
        for (RDFDataUnit rdfInput : rdfInputs) {
            try {
                AllTriplesDataUnitLoader loader = new AllTriplesDataUnitLoader(rdfInput);
                loaders.add(loader);
            } catch (DataUnitException e) {
                // clean up already initialized loaders
                for (AllTriplesLoader loader : loaders) {
                    ODCSFusionToolAppUtils.closeQuietly(loader);
                }
                throw new ODCSFusionToolException("Error creating triple loader from RDF data unit: " + e.getMessage(), e);
            }
        }
        return loaders;
    }

    @Override
    protected Model getMetadata() throws ODCSFusionToolException {
        Model metadata = new TreeModel();
        RepositoryResult<Statement> metadataResult;
        try {
            metadataResult = metadataInput.getConnection().getStatements(null, null, null, false);
            while (metadataResult.hasNext()) {
                metadata.add(metadataResult.next());
            }
            return metadata;
        } catch (RepositoryException | DataUnitException e) {
            throw new ODCSFusionToolException("Error when loading metadata from input", e);
        }
    }

    @Override
    protected UriMappingIterable getUriMapping() throws ODCSFusionToolException, IOException {
        // FIXME: preference of prefixes from configuration
        Set<String> preferredURIs = getPreferredURIs(
                config.getPropertyResolutionStrategies().keySet(),
                getCanonicalUrisFile(),
                config.getPreferredCanonicalURIs());

        UriMappingIterableImpl uriMapping = new UriMappingIterableImpl(preferredURIs);

        return loadSameAsLinks(sameAsInput, uriMapping, config.getSameAsLinkTypes());
    }

    // TODO: KONEC

    private static UriMappingIterable loadSameAsLinks(
            RDFDataUnit sameAsInput,
            UriMappingIterableImpl uriMapping, Set<URI> sameAsLinkTypes) throws ODCSFusionToolException {

        // TODO: extract to class

        LOG.info("Loading sameAs links...");
        RepositoryConnection connection = null;
        try {
            connection = sameAsInput.getConnection();
            long startTime = System.currentTimeMillis();
            long loadedCount = 0;
            for (URI link : sameAsLinkTypes) {
                String query = String.format("CONSTRUCT {?s <%1$s> ?o} WHERE {?s <%1$s> ?o}", link.stringValue());
                GraphQueryResult sameAsTriples = connection.prepareGraphQuery(QueryLanguage.SPARQL, query).evaluate();
                while (sameAsTriples.hasNext()) {
                    uriMapping.addLink(sameAsTriples.next());
                    loadedCount++;
                    if (loadedCount % LDFTConfigConstants.LOG_LOOP_SIZE == 0) {
                        LOG.info("... loaded {} sameAs links", loadedCount);
                    }
                }
            }
            LOG.info(String.format("Loaded & resolved %,d sameAs links in %,d ms", loadedCount, System.currentTimeMillis() - startTime));
        } catch (OpenRDFException | DataUnitException e) {
            throw new ODCSFusionToolException("Error when loading owl:sameAs links from input", e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (RepositoryException e) {
                    LOG.error("Error closing sameAs data unit connection", e);
                }
            }
        }
        return uriMapping;
    }

    @Override
    protected CloseableRDFWriter createRDFWriter() throws IOException, ODCSFusionToolException {
        return null;
        // FIXME
    }

    @Override
    protected ResourceDescriptionConflictResolver createConflictResolver(Model metadata, UriMappingIterable uriMapping) {
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
    protected void writeCanonicalURIs(UriMappingIterable uriMapping) throws IOException {
        if (config.getCanonicalURIsFileName() != null) {
            Set<String> canonicalUris = new HashSet<>();
            for (String mappedUri : uriMapping) {
                canonicalUris.add(uriMapping.getCanonicalURI(mappedUri));
            }
            canonicalUriFileReader.writeCanonicalUris(getCanonicalUrisFile(), canonicalUris);
        }
    }

    private File getCanonicalUrisFile() {
        return new File(executionContext.getResultDir(), config.getCanonicalURIsFileName());
    }

    @Override
    protected void writeSameAsLinks(UriMappingIterable uriMapping) throws IOException, ODCSFusionToolException {
        // Do nothing
    }

    private long calculateMemoryLimit() {
        return config.getMemoryLimit() != null
                ? config.getMemoryLimit()
                : FTConfigConstants.MAX_MEMORY_LIMIT;
    }
}
