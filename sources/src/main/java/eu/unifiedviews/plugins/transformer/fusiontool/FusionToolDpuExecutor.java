package eu.unifiedviews.plugins.transformer.fusiontool;

import cz.cuni.mff.odcleanstore.conflictresolution.*;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.DistanceMeasureImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.DummySourceQualityCalculator;
import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.AlternativeUriNavigator;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterableImpl;
import cz.cuni.mff.odcleanstore.fusiontool.util.CanonicalUriFileHelper;
import cz.cuni.mff.odcleanstore.fusiontool.util.MemoryProfiler;
import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.rdf.RDFDataUnit;
import eu.unifiedviews.dataunit.rdf.WritableRDFDataUnit;
import eu.unifiedviews.dpu.DPUContext;
import eu.unifiedviews.plugins.transformer.fusiontool.config.ConfigContainer;
import eu.unifiedviews.plugins.transformer.fusiontool.config.FileOutput;
import eu.unifiedviews.plugins.transformer.fusiontool.exceptions.FusionToolDpuErrorCodes;
import eu.unifiedviews.plugins.transformer.fusiontool.exceptions.FusionToolDpuException;
import eu.unifiedviews.plugins.transformer.fusiontool.io.LargeCollectionFactory;
import eu.unifiedviews.plugins.transformer.fusiontool.io.MapdbCollectionFactory;
import eu.unifiedviews.plugins.transformer.fusiontool.io.MemoryCollectionFactory;
import eu.unifiedviews.plugins.transformer.fusiontool.io.file.FileOutputWriter;
import eu.unifiedviews.plugins.transformer.fusiontool.io.file.FileOutputWriterFactory;
import eu.unifiedviews.plugins.transformer.fusiontool.util.*;
import org.openrdf.OpenRDFException;
import org.openrdf.model.*;
import org.openrdf.model.impl.TreeModel;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.query.*;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Fuses RDF data from input using ODCS Conflict Resolution and writes the output to RDF outputs.
 * Conflict resolution includes resolution of owl:sameAs link, resolution of instance-level conflicts.
 * See sample configuration files (sample-config-full.xml) for overview of all processing options.
 * @author Jan Michelfeit
 */
@Deprecated
public class FusionToolDpuExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(FusionToolDpuExecutor.class);

    /** An instance of {@link FileOutputWriterFactory}. */
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
    public FusionToolDpuExecutor(
            ConfigContainer config, DPUContext executionContext, List<RDFDataUnit> rdfInputs,
            RDFDataUnit sameAsInput, RDFDataUnit metadataInput, WritableRDFDataUnit rdfOutput) {
        this.config = config;
        this.executionContext = executionContext;
        this.rdfInputs = rdfInputs;
        this.sameAsInput = sameAsInput;
        this.metadataInput = metadataInput;
        this.rdfOutput = rdfOutput;
        this.hasFusionToolRan = false;
        this.fileOutputWrittenQuads = 0;
    }

    /**
     * Performs the actual ODCS-FusionTool task according to the given configuration.
     * @throws FusionToolDpuException error
     */
    public void runFusionTool() throws FusionToolDpuException {
        if (this.hasFusionToolRan) {
            throw new IllegalStateException("runFusionTool() can be called only once");
        }
        this.hasFusionToolRan = true;

        LargeCollectionFactory collectionFactory =
                createLargeCollectionFactory(config.getEnableFileCache());
        MemoryProfiler memoryProfiler = MemoryProfiler.createInstance(config.isProfilingOn());
        //timeProfiler.startCounter(EnumProfilingCounters.INITIALIZATION);
        List<FileOutputWriter> fileOutputWriters = null;
        try {
            // Load & resolve owl:sameAs links
            UriMappingIterable uriMapping = getURIMapping(); // TODO: canonical URI file
            AlternativeUriNavigator alternativeURINavigator = new AlternativeUriNavigator(uriMapping);
            Set<String> resolvedCanonicalURIs = collectionFactory.createSet();

            // Get iterator over subjects of relevant triples
            UriQueue queuedSubjects = getSeedSubjects(uriMapping, collectionFactory);
            final boolean isTransitive = false;

            // Initialize CR
            ConflictResolver conflictResolver = createConflictResolver(uriMapping);

            // File outputs
            fileOutputWriters = createRDFWriters(config.getFileOutputs(), config.getPrefixes());

            // Initialize triple counters
            long outputTriples = 0;
            long inputTriples = 0;

            // Load & process relevant triples (quads) subject by subject so that we can apply CR to them
            PrefixDeclBuilder nsPrefixes = new PrefixDeclBuilderImpl(config.getPrefixes());
            QuadLoader quadLoader = new QuadLoader(rdfInputs, alternativeURINavigator, nsPrefixes);
            //timeProfiler.stopAddCounter(EnumProfilingCounters.INITIALIZATION);
            while (queuedSubjects.hasNext() && !executionContext.canceled()) {
                //timeProfiler.startCounter(EnumProfilingCounters.BUFFERING);
                String uri = queuedSubjects.next();
                String canonicalURI = uriMapping.getCanonicalURI(uri);

                if (resolvedCanonicalURIs.contains(canonicalURI)) {
                    // avoid processing a URI multiple times
                    continue;
                }
                resolvedCanonicalURIs.add(canonicalURI);
                //timeProfiler.stopAddCounter(EnumProfilingCounters.BUFFERING);

                // Load quads for the given subject
                //timeProfiler.startCounter(EnumProfilingCounters.QUAD_LOADING);
                Collection<Statement> quads = quadLoader.loadQuadsForURI(canonicalURI);
                inputTriples += quads.size();
                //timeProfiler.stopAddCounter(EnumProfilingCounters.QUAD_LOADING);

                // Resolve conflicts
                //timeProfiler.startCounter(EnumProfilingCounters.CONFLICT_RESOLUTION);
                Collection<ResolvedStatement> resolvedQuads = conflictResolver.resolveConflicts(quads);
                //timeProfiler.stopAddCounter(EnumProfilingCounters.CONFLICT_RESOLUTION);
                LOG.trace("Resolved {} quads for URI <{}> resulting in {} quads (processed totally {} quads)",
                        new Object[] {quads.size(), canonicalURI, resolvedQuads.size(), inputTriples});
                outputTriples += resolvedQuads.size();

                // Add objects filtered by CR for traversal
                if (isTransitive) {
                    //timeProfiler.startCounter(EnumProfilingCounters.BUFFERING);
                    addDiscoveredObjects(queuedSubjects, resolvedQuads, uriMapping, resolvedCanonicalURIs);
                    //timeProfiler.stopAddCounter(EnumProfilingCounters.BUFFERING);
                }

                // Write result to output
                //timeProfiler.startCounter(EnumProfilingCounters.OUTPUT_WRITING);
                writeResults(resolvedQuads, fileOutputWriters);
                //timeProfiler.stopAddCounter(EnumProfilingCounters.OUTPUT_WRITING);

                memoryProfiler.capture();
            }
            if (executionContext.canceled()) {
                LOG.warn("FusionToolDpu execution has been stopped before it finished!");
            }
            LOG.info(String.format("Processed %,d quads which were resolved to %,d output quads.", inputTriples, outputTriples));

            writeCanonicalURIs(resolvedCanonicalURIs);

            //AbstractFusionToolRunner.printProfilingInformation(timeProfiler, memoryProfiler);
        } catch (ConflictResolutionException e) {
            throw new FusionToolDpuException(
                    FusionToolDpuErrorCodes.CONFLICT_RESOLUTION, "Conflict resolution error: " + e.getMessage(), e);
        } catch (RepositoryException | DataUnitException e) {
            throw new FusionToolDpuException(
                    FusionToolDpuErrorCodes.REPOSITORY_ERROR, "Repository error: " + e.getMessage(), e);
        } finally {
            if (collectionFactory != null) {
                try {
                    collectionFactory.close();
                } catch (IOException e) {
                    // do nothing
                }
            }
            if (fileOutputWriters != null) {
                for (FileOutputWriter writer : fileOutputWriters) {
                    try {
                        writer.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }
    }

    /**
     * Creates factory object for large collections depending on configuration.
     * If cache is enabled, the collection is backed by a file, otherwise kept in memory.
     * @param enableFileBuffer indicates if file buffer should be used
     * @return factory for large collections
     * @throws FusionToolDpuException I/O error
     */
    protected LargeCollectionFactory createLargeCollectionFactory(boolean enableFileBuffer) throws FusionToolDpuException {
        if (enableFileBuffer) {
            try {
                return new MapdbCollectionFactory(executionContext.getWorkingDir());
            } catch (IOException e) {
                throw new FusionToolDpuException(
                        FusionToolDpuErrorCodes.COLLECTION_BUFFER_CREATION_ERROR,
                        "Cannot initialize buffer on the filesystem.", e);
            }
        } else {
            return new MemoryCollectionFactory();
        }
    }

    /**
     * Returns mapping of URIs to their canonical URI created from owl:sameAs links loaded
     * from the given data sources.
     * @return mapping of URIs to their canonical URI
     * @throws FusionToolDpuException error
     */
    private UriMappingIterable getURIMapping() throws FusionToolDpuException {
        Set<String> preferredURIs = getPreferredURIs(
                config.getPropertyResolutionStrategies().keySet(),
                config.getPreferredCanonicalURIs());

        UriMappingIterableImpl uriMapping = new UriMappingIterableImpl(preferredURIs);
        try {
            String query = String.format("CONSTRUCT {?s <%1$s> ?o} WHERE {?s <%1$s> ?o}", OWL.SAMEAS);
            GraphQueryResult sameAsTriples = sameAsInput.getConnection().prepareGraphQuery(QueryLanguage.SPARQL, query).evaluate();
            while (sameAsTriples.hasNext()) {
                uriMapping.addLink(sameAsTriples.next());
            }
        } catch (OpenRDFException | DataUnitException e) {
            throw new FusionToolDpuException(
                    FusionToolDpuErrorCodes.SAME_AS_LOADING_ERROR, "Error when loading owl:sameAs links from input", e);
        }

        return uriMapping;
    }

    /**
     * Returns set of URIs preferred for canonical URIs.
     * The URIs are loaded from canonicalURIsInputFile if given and URIs present in settingsPreferredURIs are added.
     * @param settingsPreferredURIs URIs occurring on fusion tool configuration
     * @param preferredCanonicalURIs default set of preferred canonical URIs
     * @return set of URIs preferred for canonical URIs
     * @throws FusionToolDpuException I/O error
     */
    protected Set<String> getPreferredURIs(Set<URI> settingsPreferredURIs, Collection<String> preferredCanonicalURIs)
            throws FusionToolDpuException {

        Set<String> preferredURIs = new HashSet<String>(settingsPreferredURIs.size());
        for (URI uri : settingsPreferredURIs) {
            preferredURIs.add(uri.stringValue());
        }
        preferredURIs.addAll(preferredCanonicalURIs);

        if (config.getCanonicalURIsFileName() != null) {
            CanonicalUriFileHelper canonicalUriReader = new CanonicalUriFileHelper();
            try {
                canonicalUriReader.readCanonicalUris(getCanonicalUrisFile(), preferredURIs);
            } catch (IOException e) {
                throw new FusionToolDpuException(
                        FusionToolDpuErrorCodes.READ_CANONICAL_URI_FILE, "Cannot read canonical URIs from file", e);
            }
        }

        return preferredURIs;
    }

    /**
     * Returns collections of seed subjects, i.e. the initial URIs for which
     * corresponding quads are loaded and resolved.
     * @param uriMapping canonical URI mapping
     * @param collectionFactory collection factory
     * @return collection of seed subjects
     * @throws FusionToolDpuException query error
     */
    private UriQueue getSeedSubjects(URIMapping uriMapping, LargeCollectionFactory collectionFactory)
            throws FusionToolDpuException {

        UriQueueImpl seedSubjects = new UriQueueImpl(collectionFactory);
        String query = "SELECT DISTINCT ?s WHERE {?s ?p ?o}";
        TupleQueryResult queryResult = null;
        try {
            for (RDFDataUnit rdfInput : rdfInputs) {
                queryResult = rdfInput.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();

                String variableName = null;
                while (queryResult.hasNext()) {
                    BindingSet bindings = queryResult.next();
                    if (variableName == null) {
                        variableName = bindings.getBindingNames().iterator().next();
                    }

                    Value subject = bindings.getValue(variableName);
                    String uri = ODCSUtils.isVirtuosoBlankNode(subject) ? ODCSUtils.getVirtuosoURIForBlankNode((BNode) subject) : subject.stringValue();
                    if (uri != null) {
                        String canonicalURI = uriMapping.getCanonicalURI(subject.stringValue());
                        seedSubjects.add(canonicalURI); // only store canonical URIs to save space
                    }
                }
            }
        } catch (MalformedQueryException e) {
            throw new FusionToolDpuException(
                    FusionToolDpuErrorCodes.INVALID_SEED_RESOURCE_QUERY, "Invalid seed resource SPARQL query", e);
        } catch (Exception e) {
            throw new FusionToolDpuException(
                    FusionToolDpuErrorCodes.SEED_SUBJECTS_LOADING_ERROR, "Error when loading initial subjects", e);
        } finally {
            if (queryResult != null) {
                try {
                    queryResult.close();
                } catch (QueryEvaluationException e) {
                    // do nothing
                }
            }
        }

        if (config.isProfilingOn() && LOG.isDebugEnabled()) {
            // only when debug is enabled, this may be expensive when using file cache
            LOG.debug(String.format("ODCS-FusionTool: loaded %,d seed resources", seedSubjects.size()));
        }

        return seedSubjects;
    }

    /**
     * Creates initialized conflict resolver.
     * @param uriMapping mapping of URIs to their canonical URI
     * @return initialized conflict resolver
     */
    protected ConflictResolver createConflictResolver(UriMappingIterable uriMapping) throws FusionToolDpuException {
        Model metadata = new TreeModel();
        RepositoryResult<Statement> metadataResult = null;
        try {
            metadataResult = metadataInput.getConnection().getStatements(null, null, null, false);
            while (metadataResult.hasNext()) {
                metadata.add(metadataResult.next());
            }
        } catch (RepositoryException | DataUnitException e) {
            throw new FusionToolDpuException(
                    FusionToolDpuErrorCodes.METADATA_LOADING_ERROR, "Error when loading metadata from input", e);
        }

        ResolutionFunctionRegistry registry = ConflictResolverFactory.createInitializedResolutionFunctionRegistry(
                new DummySourceQualityCalculator(),
                config.getAgreeCoefficient(),
                new DistanceMeasureImpl());

        ConflictResolver conflictResolver = ConflictResolverFactory.configureResolver()
                .setResolutionFunctionRegistry(registry)
                .setResolvedGraphsURIPrefix(config.getResultDataURIPrefix())
                .setMetadata(metadata)
                .setURIMapping(uriMapping)
                .setDefaultResolutionStrategy(config.getDefaultResolutionStrategy())
                .setPropertyResolutionStrategies(config.getPropertyResolutionStrategies())
                .create();
        return conflictResolver;
    }

    /**
     * Adds URIs from objects of resolved statements to the given collection of queued subjects.
     * Only URIs that haven't been resolved already are added.
     * @param queuedSubjects collection where URIs are added
     * @param resolvedStatements resolved statements whose objects are added to queued subjects
     * @param uriMapping mapping to canonical URIs
     * @param resolvedCanonicalURIs set of already resolved URIs
     */
    protected void addDiscoveredObjects(
            UriQueue queuedSubjects,
            Collection<ResolvedStatement> resolvedStatements,
            UriMappingIterable uriMapping,
            Set<String> resolvedCanonicalURIs) {

        for (ResolvedStatement resolvedStatement : resolvedStatements) {
            Value object = resolvedStatement.getStatement().getObject();
            String uri = ODCSUtils.isVirtuosoBlankNode(object) ? ODCSUtils.getVirtuosoURIForBlankNode((BNode) object) : object.stringValue();
            if (uri == null) {
                // a literal or something, skip it
                continue;
            }

            // only add canonical URIs to save space
            String canonicalURI = uriMapping.getCanonicalURI(uri);

            // only add new URIs
            if (!resolvedCanonicalURIs.contains(canonicalURI)) {
                queuedSubjects.add(canonicalURI);
            }
        }
    }

    /**
     * Writes conflict resolution results to DPU outputs.
     * @param resolvedQuads conflict resolution results
     * @param fileOutputWriters file output writers
     */
    protected void writeResults(
            Collection<ResolvedStatement> resolvedQuads,
            List<FileOutputWriter> fileOutputWriters) throws RepositoryException, DataUnitException {
        RepositoryConnection connection = rdfOutput.getConnection();
        for (ResolvedStatement resolvedStatement : resolvedQuads) {
            Statement statement = resolvedStatement.getStatement();
            connection.add(statement.getSubject(), statement.getPredicate(), statement.getObject());

            Long maxOutputQuads = config.getMaxOutputTriples();
            if (maxOutputQuads == null || this.fileOutputWrittenQuads < maxOutputQuads) {
                for (FileOutputWriter writer : fileOutputWriters) {
                    try {
                        writer.writeResolvedStatements(resolvedQuads.iterator());
                    } catch (IOException e) {
                        LOG.warn("Error when writing {} to file output: {}", resolvedStatement, e.getMessage());
                    }
                }
                this.fileOutputWrittenQuads++;
            }
        }
    }


    /**
     * Writes canonical URIs to a file specified in configuration.
     * @param resolvedCanonicalURIs resolved canonical URIs
     * @throws FusionToolDpuException I/O error
     */
    protected void writeCanonicalURIs(Set<String> resolvedCanonicalURIs) throws FusionToolDpuException {
        if (config.getCanonicalURIsFileName() != null) {
            CanonicalUriFileHelper canonicalUriReader = new CanonicalUriFileHelper();
            try {
                canonicalUriReader.writeCanonicalUris(getCanonicalUrisFile(), resolvedCanonicalURIs);
            } catch (IOException e) {
                throw new FusionToolDpuException(
                        FusionToolDpuErrorCodes.WRITE_CANONICAL_URI_FILE, "Cannot write canonical URIs from file", e);
            }
        }
    }

    /**
     * Creates and initializes output writers.
     * @param outputs specification of data outputs
     * @param nsPrefixes namespace prefix mappings
     * @return output writers
     * @throws FusionToolDpuException configuration error
     */
    protected List<FileOutputWriter> createRDFWriters(List<FileOutput> outputs, Map<String, String> nsPrefixes)
            throws FusionToolDpuException {

        try {
            List<FileOutputWriter> writers = new LinkedList<FileOutputWriter>();
            for (FileOutput output : outputs) {
                FileOutputWriter writer = RDF_WRITER_FACTORY.createRDFWriter(output, executionContext.getResultDir());
                writers.add(writer);

                for (Map.Entry<String, String> entry : nsPrefixes.entrySet()) {
                    writer.addNamespace(entry.getKey(), entry.getValue());
                }
            }
            return writers;
        } catch (IOException e) {
            throw new FusionToolDpuException(FusionToolDpuErrorCodes.FILE_OUTPUT_WRITER_CREATION,
                    "Error when creating file output writers");
        }
    }

    private File getCanonicalUrisFile() {
        return new File(executionContext.getGlobalDirectory(), config.getCanonicalURIsFileName());
    }
}
