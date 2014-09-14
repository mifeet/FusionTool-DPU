package eu.unifiedviews.plugins.transformer.fusiontool;

import com.google.common.collect.*;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionStrategy;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolutionStrategyImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolvedStatementImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.EmptyMetadataModel;
import cz.cuni.mff.odcleanstore.fusiontool.FusionComponentFactory;
import cz.cuni.mff.odcleanstore.fusiontool.FusionExecutor;
import cz.cuni.mff.odcleanstore.fusiontool.LDFusionToolExecutor;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescription;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl.NestedResourceDescriptionResolution;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl.ResourceDescriptionImpl;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterableImpl;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.InputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter.ResourceDescriptionFilter;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriter;
import cz.cuni.mff.odcleanstore.fusiontool.writers.UriMappingWriter;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;
import eu.unifiedviews.dataunit.rdf.RDFDataUnit;
import eu.unifiedviews.dpu.DPUContext;
import eu.unifiedviews.plugins.transformer.fusiontool.config.ConfigContainer;
import eu.unifiedviews.plugins.transformer.fusiontool.config.ConfigContainerImpl;
import eu.unifiedviews.plugins.transformer.fusiontool.config.FTConfigConstants;
import eu.unifiedviews.plugins.transformer.fusiontool.io.DataUnitRDFWriterWithMetadata;
import eu.unifiedviews.plugins.transformer.fusiontool.util.MockRDFDataUnit;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.repository.RepositoryException;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static eu.unifiedviews.plugins.transformer.fusiontool.testutils.ContextAwareStatementIsEqual.STATEMENT_TO_MATCHER;
import static eu.unifiedviews.plugins.transformer.fusiontool.testutils.ContextAwareStatementIsEqual.contextAwareStatementIsEqual;
import static eu.unifiedviews.plugins.transformer.fusiontool.testutils.FTDPUTestUtils.*;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FusionToolDpuComponentFactoryTest {

    public static final ValueFactoryImpl VF = ValueFactoryImpl.getInstance();

    @Rule
    public TemporaryFolder testDir = new TemporaryFolder();

    private DPUContext dpuContext;
    private ImmutableList<RDFDataUnit> rdfInputs;
    private MockRDFDataUnit metadataDataUnit;
    private UriMappingIterableImpl uriMapping;
    private MockRDFDataUnit outputDataUnit;
    private RDFDataUnit sameAsDataUnit;

    @Before
    public void setUp() throws Exception {
        dpuContext = mock(DPUContext.class);

        uriMapping = new UriMappingIterableImpl(ImmutableSet.of(
                createHttpUri("a1").stringValue(),
                createHttpUri("b1").stringValue()));
        uriMapping.addLink(createHttpUri("a1"), createHttpUri("a2"));
        uriMapping.addLink(createHttpUri("b1"), createHttpUri("b2"));
        uriMapping.addLink(createHttpUri("b2"), createHttpUri("b3"));

        rdfInputs = ImmutableList.of(mock(RDFDataUnit.class), mock(RDFDataUnit.class));
        metadataDataUnit = new MockRDFDataUnit(ImmutableList.of(createStatement(), createStatement()));
        outputDataUnit = new MockRDFDataUnit();
        sameAsDataUnit = mock(RDFDataUnit.class);
    }

    @After
    public void tearDown() throws Exception {
        metadataDataUnit.close();
        outputDataUnit.close();
        if (sameAsDataUnit instanceof MockRDFDataUnit) {
            ((MockRDFDataUnit) sameAsDataUnit).close();
        }
    }

    @Test
    public void getsMetadata() throws Exception {
        FusionToolDpuComponentFactory componentFactory = getComponentFactory();

        Model metadata = componentFactory.getMetadata();

        List<Statement> expectedStatements = metadataDataUnit.getAllStatements();
        assertThat(metadata, containsInAnyOrder(Lists.transform(expectedStatements, STATEMENT_TO_MATCHER)));
    }

    @Test
    public void getsExecutorTimeProfiler() throws Exception {
        FusionToolDpuComponentFactory componentFactory = getComponentFactory();
        assertThat(componentFactory.getExecutorTimeProfiler(), notNullValue());
    }

    @Test
    public void getsNoOpCanonicalUriWriterWhenCanonicalUriFileIsNull() throws Exception {
        ConfigContainer config = mock(ConfigContainer.class);
        when(config.getCanonicalURIsFileName()).thenReturn(null);

        FusionToolDpuComponentFactory componentFactory = getComponentFactory(config);
        UriMappingWriter writer = componentFactory.getCanonicalUriWriter(uriMapping);

        writer.write(uriMapping);
        assertThat(testDir.getRoot().list(), is(new String[0]));
    }

    @Test
    public void getFileWritingCanonicalUriWriterWhenCanonicalUriFileIsGiven() throws Exception {
        File outputFile = testDir.newFile();
        ConfigContainer config = mock(ConfigContainer.class);
        when(config.getCanonicalURIsFileName()).thenReturn(outputFile.getAbsolutePath());

        FusionToolDpuComponentFactory componentFactory = getComponentFactory(config);
        UriMappingWriter writer = componentFactory.getCanonicalUriWriter(uriMapping);

        writer.write(uriMapping);
        List<String> writtenUris = Files.readAllLines(outputFile.toPath(), Charset.defaultCharset());
        assertThat(writtenUris, containsInAnyOrder(
                createHttpUri("a1").stringValue(),
                createHttpUri("b1").stringValue()
        ));
    }

    @Test
    public void getsNoOpSameAsLinkWriter() throws Exception {
        FusionToolDpuComponentFactory componentFactory = getComponentFactory();
        UriMappingWriter writer = componentFactory.getSameAsLinksWriter();

        writer.write(uriMapping);
        assertThat(testDir.getRoot().list(), is(new String[0]));
    }

    @Test
    public void getsConflictResolver() throws Exception {
        ConfigContainerImpl config = new ConfigContainerImpl();
        FusionToolDpuComponentFactory componentFactory = getComponentFactory(config);
        ResourceDescriptionConflictResolver conflictResolver = componentFactory.getConflictResolver(new EmptyMetadataModel(), uriMapping);
        assertThat(conflictResolver, notNullValue());
    }

    @Test
    public void getsTripleRDFWriterWhenWritingMetadataIsDisabled() throws Exception {
        ConfigContainer config = mock(ConfigContainer.class);
        when(config.getWriteMetadata()).thenReturn(false);

        // Act
        FusionToolDpuComponentFactory componentFactory = getComponentFactory(config);
        try (CloseableRDFWriter writer = componentFactory.getRDFWriter()) {

            // Assert
            Statement statement = createStatement();
            ResolvedStatement resolvedStatement = new ResolvedStatementImpl(statement, 0.5, ImmutableList.of((Resource) getUniqueURI()));
            writer.write(resolvedStatement);

            Statement actualStatement = Iterables.getOnlyElement(outputDataUnit.getAllStatements());
            MatcherAssert.assertThat(actualStatement, is(statement));
            MatcherAssert.assertThat(actualStatement.getContext(), is((Resource) outputDataUnit.getDataGraphURI()));
            outputDataUnit.getAllStatements();
        }
    }

    @Test
    public void getsQuadRDFWriterWhenWritingMetadataIsEnabled() throws Exception {
        ConfigContainer config = mock(ConfigContainer.class);
        when(config.getWriteMetadata()).thenReturn(true);

        // Act
        FusionToolDpuComponentFactory componentFactory = getComponentFactory(config);
        try (CloseableRDFWriter writer = componentFactory.getRDFWriter()) {

            // Assert
            Statement statement = createStatement();
            ResolvedStatement resolvedStatement = new ResolvedStatementImpl(statement, 0.5, ImmutableList.of((Resource) createHttpUri("source1")));
            writer.write(resolvedStatement);

            URI metadataGraph = outputDataUnit.addNewDataGraph(DataUnitRDFWriterWithMetadata.METADATA_GRAPH_NAME);
            URI resultGraph = Iterables.getOnlyElement(Sets.difference(Sets.newHashSet(outputDataUnit.getAddedGraphs().values()), ImmutableSet.of(metadataGraph)));

            List<Statement> expectedStatements = ImmutableList.of(
                    VF.createStatement(statement.getSubject(), statement.getPredicate(), statement.getObject(), resultGraph),
                    VF.createStatement(resultGraph, ODCS.QUALITY, VF.createLiteral(0.5), metadataGraph),
                    VF.createStatement(resultGraph, ODCS.SOURCE_GRAPH, createHttpUri("source1"), metadataGraph)
            );
            List<Statement> actualStatements = outputDataUnit.getAllStatements();
            MatcherAssert.assertThat(actualStatements, containsInAnyOrder(Lists.transform(expectedStatements, STATEMENT_TO_MATCHER)));
        }
    }

    @Test
    public void getsUriMapping() throws Exception {
        // Arrange
        sameAsDataUnit = new MockRDFDataUnit(ImmutableList.of(
                VF.createStatement(createHttpUri("a1"), OWL.SAMEAS, createHttpUri("a2")),
                VF.createStatement(createHttpUri("a2"), OWL.SAMEAS, createHttpUri("a3")),
                VF.createStatement(createHttpUri("b1"), OWL.SAMEAS, createHttpUri("b2")),
                VF.createStatement(createHttpUri("b2"), OWL.SAMEAS, createHttpUri("b3")),
                VF.createStatement(createHttpUri("p1"), OWL.SAMEAS, createHttpUri("p2")),
                VF.createStatement(createHttpUri("p2"), OWL.SAMEAS, createHttpUri("p3"))
        ));

        File resultDir = testDir.newFolder("result");
        when(dpuContext.getResultDir()).thenReturn(resultDir);

        ConfigContainer config = mock(ConfigContainer.class);
        when(config.getSameAsLinkTypes()).thenReturn(ImmutableSet.of(OWL.SAMEAS));
        when(config.getPropertyResolutionStrategies()).thenReturn(ImmutableMap.<URI, ResolutionStrategy>of(createHttpUri("p2"), new ResolutionStrategyImpl()));
        when(config.getPreferredCanonicalURIs()).thenReturn(ImmutableSet.of(createHttpUri("a2").stringValue()));
        Files.write(new File(resultDir, "canonicalUris.txt").toPath(), ImmutableList.of(createHttpUri("b2").stringValue()), Charset.defaultCharset());
        when(config.getCanonicalURIsFileName()).thenReturn("canonicalUris.txt");

        // Act
        FusionToolDpuComponentFactory componentFactory = getComponentFactory(config);
        UriMappingIterable uriMappingIterable = componentFactory.getUriMapping();

        // Assert
        assertThat(uriMappingIterable.mapResource(createHttpUri("a1")), is((Resource) createHttpUri("a2")));
        assertThat(uriMappingIterable.mapResource(createHttpUri("a2")), is((Resource) createHttpUri("a2")));
        assertThat(uriMappingIterable.mapResource(createHttpUri("a3")), is((Resource) createHttpUri("a2")));
        assertThat(uriMappingIterable.mapResource(createHttpUri("b1")), is((Resource) createHttpUri("b2")));
        assertThat(uriMappingIterable.mapResource(createHttpUri("b2")), is((Resource) createHttpUri("b2")));
        assertThat(uriMappingIterable.mapResource(createHttpUri("b3")), is((Resource) createHttpUri("b2")));
        assertThat(uriMappingIterable.mapResource(createHttpUri("p1")), is((Resource) createHttpUri("p2")));
        assertThat(uriMappingIterable.mapResource(createHttpUri("p2")), is((Resource) createHttpUri("p2")));
        assertThat(uriMappingIterable.mapResource(createHttpUri("p3")), is((Resource) createHttpUri("p2")));
        assertThat(Iterables.size(uriMappingIterable), is(6));
    }

    @Test
    public void getsExecutorWithEmptyInputFilter() throws Exception {
        ConfigContainer config = mock(ConfigContainer.class);
        when(config.getMaxOutputTriples()).thenReturn(101L);

        // Act
        FusionToolDpuComponentFactory componentFactory = getComponentFactory(config);
        FusionExecutor executor = componentFactory.getExecutor(uriMapping);

        // Assert
        assertThat(executor, instanceOf(LDFusionToolExecutor.class));
        LDFusionToolExecutor typedExecutor = (LDFusionToolExecutor) executor;
        assertThat(typedExecutor.getMaxOutputTriples(), is(101L));
        ResourceDescriptionFilter filter = typedExecutor.getResourceDescriptionFilter();
        assertTrue(filter.accept(new ResourceDescriptionImpl(createHttpUri("r1"), ImmutableList.of(createHttpStatement("r1", "p", "o")))));
        assertTrue(filter.accept(new ResourceDescriptionImpl(createHttpUri("r2"), ImmutableList.of(VF.createStatement(createHttpUri("r2"), RDF.TYPE, createHttpUri("o"))))));
        assertTrue(filter.accept(new ResourceDescriptionImpl(createHttpUri("a2"), ImmutableList.of(createHttpStatement("a2", "p", "o")))));
    }

    @Test
    public void getsExecutorWithClassAndMappingInputFilters() throws Exception {
        ConfigContainer config = mock(ConfigContainer.class);
        when(config.getMaxOutputTriples()).thenReturn(101L);
        when(config.getRequiredClassOfProcessedResources()).thenReturn(createHttpUri("c"));
        when(config.getOutputMappedSubjectsOnly()).thenReturn(true);

        // Act
        FusionToolDpuComponentFactory componentFactory = getComponentFactory(config);
        FusionExecutor executor = componentFactory.getExecutor(uriMapping);

        // Assert
        assertThat(executor, instanceOf(LDFusionToolExecutor.class));
        LDFusionToolExecutor typedExecutor = (LDFusionToolExecutor) executor;
        assertThat(typedExecutor.getMaxOutputTriples(), is(101L));
        ResourceDescriptionFilter filter = typedExecutor.getResourceDescriptionFilter();
        assertFalse(filter.accept(new ResourceDescriptionImpl(createHttpUri("r1"), ImmutableList.of(createHttpStatement("r1", "p", "o")))));
        assertFalse(filter.accept(new ResourceDescriptionImpl(createHttpUri("r2"), ImmutableList.of(VF.createStatement(createHttpUri("r2"), RDF.TYPE, createHttpUri("c"))))));
        assertFalse(filter.accept(new ResourceDescriptionImpl(createHttpUri("a2"), ImmutableList.of(createHttpStatement("a2", "p", "o")))));
        assertTrue(filter.accept(new ResourceDescriptionImpl(createHttpUri("b2"), ImmutableList.of(VF.createStatement(createHttpUri("b2"), RDF.TYPE, createHttpUri("c"))))));
    }

    @Test(expected = IllegalStateException.class)
    public void getInputLoaderRequiresLocalCopyProcessing() throws Exception {
        ConfigContainer config = mock(ConfigContainer.class);
        when(config.isLocalCopyProcessing()).thenReturn(false);

        // Act
        FusionToolDpuComponentFactory componentFactory = getComponentFactory(config);
        componentFactory.getInputLoader();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void getsExternalSortingInputLoader() throws Exception {
        // Arrange
        File workingDir = testDir.newFolder("wd");
        when(dpuContext.getWorkingDir()).thenReturn(workingDir);
        ConfigContainer config = mock(ConfigContainer.class);
        when(config.isLocalCopyProcessing()).thenReturn(true);
        when(config.getParserConfig()).thenReturn(FTConfigConstants.DEFAULT_FILE_PARSER_CONFIG);
        when(config.getPropertyResolutionStrategies()).thenReturn(ImmutableMap.<URI, ResolutionStrategy>of(
                createHttpUri("resourceDescriptionProperty"), new ResolutionStrategyImpl(NestedResourceDescriptionResolution.getName())
        ));
        ImmutableList<Statement> inputTriples1 = ImmutableList.of(
                createHttpStatement("a1", "pa1", "oa1", "dataGraph1"),
                createHttpStatement("b1", "resourceDescriptionProperty", "dependent", "dataGraph1"),
                createHttpStatement("c1", "pc1", "oc1", "otherGraphShouldBeIgnored")
        );
        ImmutableList<Statement> inputTriples2 = ImmutableList.of(
                createHttpStatement("a2", "pa2", "oa2", "dataGraph2"),
                createHttpStatement("dependent", "pd", "od", "dataGraph2")
        );

        // Act
        Map<URI, Collection<Statement>> resourceDescriptions;
        try (MockRDFDataUnit source1 = dataUnitWithGraph(inputTriples1, createHttpUri("dataGraph1"));
             MockRDFDataUnit source2 = dataUnitWithGraph(inputTriples2, createHttpUri("dataGraph2"));
             InputLoader inputLoader = getComponentFactory(config, ImmutableList.of(source1, source2)).getInputLoader()
        ) {
            inputLoader.initialize(uriMapping);
            resourceDescriptions = collectResourcesDescriptions(inputLoader);
        }

        // Assert
        assertThat(resourceDescriptions.keySet(), containsInAnyOrder(createHttpUri("a1"), createHttpUri("b1"), createHttpUri("dependent")));
        assertThat(resourceDescriptions.get(createHttpUri("a1")), containsInAnyOrder(
                contextAwareStatementIsEqual(createHttpStatement("a1", "pa1", "oa1", "dataGraph1")),
                contextAwareStatementIsEqual(createHttpStatement("a2", "pa2", "oa2", "dataGraph2"))));
        assertThat(resourceDescriptions.get(createHttpUri("b1")), containsInAnyOrder(
                contextAwareStatementIsEqual(createHttpStatement("b1", "resourceDescriptionProperty", "dependent", "dataGraph1")),
                contextAwareStatementIsEqual(createHttpStatement("dependent", "pd", "od", "dataGraph2"))));
        assertThat(resourceDescriptions.get(createHttpUri("dependent")), contains(
                contextAwareStatementIsEqual(createHttpStatement("dependent", "pd", "od", "dataGraph2"))));
    }

    private MockRDFDataUnit dataUnitWithGraph(ImmutableList<Statement> inputTriples1, URI dataGraphUri) throws RepositoryException {
        MockRDFDataUnit dataUnit = new MockRDFDataUnit(inputTriples1);
        dataUnit.setDataGraphURI(dataGraphUri);
        return dataUnit;
    }

    private Map<URI, Collection<Statement>> collectResourcesDescriptions(InputLoader inputLoader) throws LDFusionToolException {
        Map<URI, Collection<Statement>> result = new HashMap<>();
        while (inputLoader.hasNext()) {
            ResourceDescription resourceDescription = inputLoader.next();
            result.put((URI) resourceDescription.getResource(), resourceDescription.getDescribingStatements());
        }
        return result;
    }

    private FusionToolDpuComponentFactory getComponentFactory() {
        return getComponentFactory(mock(ConfigContainer.class));
    }

    private FusionComponentFactory getComponentFactory(ConfigContainer config, List<? extends RDFDataUnit> factoryRdfInputs) {
        return new FusionToolDpuComponentFactory(
                config,
                dpuContext,
                factoryRdfInputs,
                sameAsDataUnit,
                metadataDataUnit,
                outputDataUnit);
    }

    private FusionToolDpuComponentFactory getComponentFactory(ConfigContainer config) {
        return new FusionToolDpuComponentFactory(config, dpuContext, rdfInputs, sameAsDataUnit, metadataDataUnit, outputDataUnit);
    }

}
