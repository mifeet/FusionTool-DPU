package eu.unifiedviews.plugins.transformer.fusiontool;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolvedStatementImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.EmptyMetadataModel;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterableImpl;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriter;
import cz.cuni.mff.odcleanstore.fusiontool.writers.UriMappingWriter;
import eu.unifiedviews.dataunit.rdf.RDFDataUnit;
import eu.unifiedviews.dpu.DPUContext;
import eu.unifiedviews.plugins.transformer.fusiontool.config.ConfigContainer;
import eu.unifiedviews.plugins.transformer.fusiontool.config.ConfigContainerImpl;
import eu.unifiedviews.plugins.transformer.fusiontool.testutils.ContextAwareStatementIsEqual;
import eu.unifiedviews.plugins.transformer.fusiontool.testutils.FTDPUTestUtils;
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

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FusionToolDpuComponentFactoryTest {

    @Rule
    public TemporaryFolder testDir = new TemporaryFolder();

    private DPUContext dpuContext;
    private File resultDir;
    private ImmutableList<RDFDataUnit> rdfInputs;
    private MockRDFDataUnit metadataDataUnit;
    private UriMappingIterableImpl uriMapping;
    private MockRDFDataUnit outputDataUnit;

    @Before
    public void setUp() throws Exception {
        dpuContext = mock(DPUContext.class);
        //resultDir = testDir.newFolder("result");
        //when(dpuContext.getResultDir()).thenReturn(resultDir);

        metadataDataUnit = new MockRDFDataUnit(FTDPUTestUtils.createRepository(ImmutableList.of(
                FTDPUTestUtils.createStatement(),
                FTDPUTestUtils.createStatement())));


        uriMapping = new UriMappingIterableImpl(ImmutableSet.of(
                FTDPUTestUtils.createHttpUri("a1").stringValue(),
                FTDPUTestUtils.createHttpUri("b1").stringValue()));
        uriMapping.addLink(FTDPUTestUtils.createHttpUri("a1"), FTDPUTestUtils.createHttpUri("a2"));
        uriMapping.addLink(FTDPUTestUtils.createHttpUri("b1"), FTDPUTestUtils.createHttpUri("b2"));
        uriMapping.addLink(FTDPUTestUtils.createHttpUri("b2"), FTDPUTestUtils.createHttpUri("b3"));

        outputDataUnit = new MockRDFDataUnit(FTDPUTestUtils.createRepository(Collections.<Statement>emptySet()));

        rdfInputs = ImmutableList.of(mock(RDFDataUnit.class), mock(RDFDataUnit.class));

    }

    @After
    public void tearDown() throws Exception {
        metadataDataUnit.close();
        outputDataUnit.close();
    }

    @Test
    public void getsMetadata() throws Exception {
        FusionToolDpuComponentFactory componentFactory = getComponentFactory();

        Model metadata = componentFactory.getMetadata();

        assertThat(
                metadata,
                containsInAnyOrder(Lists.transform(metadataDataUnit.getAllStatements(), ContextAwareStatementIsEqual.STATEMENT_TO_MATCHER)));
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
                FTDPUTestUtils.createHttpUri("a1").stringValue(),
                FTDPUTestUtils.createHttpUri("b1").stringValue()
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
            Statement statement = FTDPUTestUtils.createStatement();
            ResolvedStatement resolvedStatement = new ResolvedStatementImpl(statement, 0.5, ImmutableList.of((Resource) FTDPUTestUtils.getUniqueURI()));
            writer.write(resolvedStatement);

            Statement actualStatement = Iterables.getOnlyElement(outputDataUnit.getAllStatements());
            MatcherAssert.assertThat(actualStatement, is(statement));
            MatcherAssert.assertThat(actualStatement.getContext(), is((Resource) outputDataUnit.getBaseDataGraphURI()));
            outputDataUnit.getAllStatements();
        }
    }

    @Test
    public void getsQuadRDFWriterWhenWritingMetadataIsEnabled() throws Exception {
        ConfigContainer config = mock(ConfigContainer.class);
        when(config.getWriteMetadata()).thenReturn(false);

        // Act
        FusionToolDpuComponentFactory componentFactory = getComponentFactory(config);
        try (CloseableRDFWriter writer = componentFactory.getRDFWriter()) {

            // Assert
            Statement statement = FTDPUTestUtils.createStatement();
            ResolvedStatement resolvedStatement = new ResolvedStatementImpl(statement, 0.5, ImmutableList.of((Resource) FTDPUTestUtils.getUniqueURI()));
            writer.write(resolvedStatement);

            Statement actualStatement = Iterables.getOnlyElement(outputDataUnit.getAllStatements());
            MatcherAssert.assertThat(actualStatement, is(statement));
            MatcherAssert.assertThat(actualStatement.getContext(), is((Resource) outputDataUnit.getBaseDataGraphURI()));
            outputDataUnit.getAllStatements();
        }
    }

    private FusionToolDpuComponentFactory getComponentFactory() {
        return getComponentFactory(mock(ConfigContainer.class));
    }

    private FusionToolDpuComponentFactory getComponentFactory(ConfigContainer config) {
        return new FusionToolDpuComponentFactory(
                config,
                dpuContext,
                rdfInputs,
                mock(RDFDataUnit.class),
                metadataDataUnit,
                outputDataUnit);
    }

}
