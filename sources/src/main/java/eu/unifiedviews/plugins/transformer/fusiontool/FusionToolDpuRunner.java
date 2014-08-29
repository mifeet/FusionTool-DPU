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
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl.NestedResourceDescriptionQualityCalculatorImpl;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl.ResourceDescriptionConflictResolverImpl;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterableImpl;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.InputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriter;
import cz.cuni.mff.odcleanstore.vocabulary.ODCSInternal;
import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.rdf.RDFDataUnit;
import eu.unifiedviews.dataunit.rdf.WritableRDFDataUnit;
import eu.unifiedviews.dpu.DPUContext;
import eu.unifiedviews.plugins.transformer.fusiontool.config.ConfigContainer;
import eu.unifiedviews.plugins.transformer.fusiontool.io.file.FileOutputWriterFactory;
import org.openrdf.OpenRDFException;
import org.openrdf.model.Model;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.TreeModel;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
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
    protected InputLoader getInputLoader() throws IOException, ODCSFusionToolException {
        return null;
        // FIXME
    }

    @Override
    protected CloseableRDFWriter createRDFWriter() throws IOException, ODCSFusionToolException {
        return null;
        // FIXME
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

        try {
            for (URI link : config.getSameAsLinkTypes()) {
                String query = String.format("CONSTRUCT {?s <%1$s> ?o} WHERE {?s <%1$s> ?o}", link.stringValue());
                GraphQueryResult sameAsTriples = sameAsInput.getConnection().prepareGraphQuery(QueryLanguage.SPARQL, query).evaluate();
                while (sameAsTriples.hasNext()) {
                    uriMapping.addLink(sameAsTriples.next());
                }
            }
        } catch (OpenRDFException | DataUnitException e) {
            throw new ODCSFusionToolException("Error when loading owl:sameAs links from input", e);
        }
        return uriMapping;
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
    protected ODCSFusionToolExecutor createExecutor(UriMappingIterable uriMapping) {
        return null;
        // FIXME
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
        // FIXME
    }
}
