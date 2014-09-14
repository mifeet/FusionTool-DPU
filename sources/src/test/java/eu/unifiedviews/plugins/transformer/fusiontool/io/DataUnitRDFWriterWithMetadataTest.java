package eu.unifiedviews.plugins.transformer.fusiontool.io;

import com.google.common.collect.*;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolvedStatementImpl;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;
import eu.unifiedviews.dataunit.rdf.WritableRDFDataUnit;
import eu.unifiedviews.plugins.transformer.fusiontool.testutils.ContextAwareStatementIsEqual;
import eu.unifiedviews.plugins.transformer.fusiontool.testutils.FTDPUTestUtils;
import eu.unifiedviews.plugins.transformer.fusiontool.util.MockRDFDataUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.RepositoryConnection;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.*;

public class DataUnitRDFWriterWithMetadataTest {
    public static final ValueFactoryImpl VF = ValueFactoryImpl.getInstance();
    private DataUnitRDFWriterWithMetadata writer;
    private MockRDFDataUnit dataUnit;

    @Before
    public void setUp() throws Exception {
        dataUnit = new MockRDFDataUnit();
        writer = new DataUnitRDFWriterWithMetadata(dataUnit);
    }

    @After
    public void tearDown() throws Exception {
        writer.close();
        dataUnit.close();
    }

    @Test
    public void writesStatementToDefaultGraph() throws Exception {
        Statement statement = FTDPUTestUtils.createStatement();

        writer.write(statement);

        Statement actualStatement = Iterables.getOnlyElement(dataUnit.getAllStatements());
        assertThat(actualStatement, is(statement));
        assertThat(actualStatement.getContext(), is((Resource) dataUnit.getDataGraphURI()));
    }

    @Test
    public void writesResolvedStatementToDefaultGraph() throws Exception {
        Statement statement = FTDPUTestUtils.createStatement();
        ResolvedStatement resolvedStatement = new ResolvedStatementImpl(
                statement,
                0.5,
                ImmutableList.<Resource>of(FTDPUTestUtils.createHttpUri("source1"), FTDPUTestUtils.createHttpUri("source2")));

        // Act
        writer.write(resolvedStatement);

        // Assert
        Collection<URI> addedGraphs = dataUnit.getAddedGraphs().values();
        assertThat(addedGraphs, hasSize(2));
        URI metadataGraph = dataUnit.addNewDataGraph(DataUnitRDFWriterWithMetadata.METADATA_GRAPH_NAME);
        assertThat(addedGraphs, hasItem(metadataGraph));
        URI resultGraph = Iterables.getOnlyElement(Sets.difference(Sets.newHashSet(addedGraphs), ImmutableSet.of(metadataGraph)));

        List<Statement> expectedStatements = ImmutableList.of(
                VF.createStatement(statement.getSubject(), statement.getPredicate(), statement.getObject(), resultGraph),
                VF.createStatement(resultGraph, ODCS.QUALITY, VF.createLiteral(0.5), metadataGraph),
                VF.createStatement(resultGraph, ODCS.SOURCE_GRAPH, FTDPUTestUtils.createHttpUri("source1"), metadataGraph),
                VF.createStatement(resultGraph, ODCS.SOURCE_GRAPH, FTDPUTestUtils.createHttpUri("source2"), metadataGraph)
        );
        List<Statement> actualStatements = dataUnit.getAllStatements();
        assertThat(actualStatements, containsInAnyOrder(Lists.transform(expectedStatements, ContextAwareStatementIsEqual.STATEMENT_TO_MATCHER)));
    }

    @Test
    public void closesConnection() throws Exception {
        // Arrange
        WritableRDFDataUnit dataUnit = mock(WritableRDFDataUnit.class);
        when(dataUnit.getBaseDataGraphURI()).thenReturn(FTDPUTestUtils.getUniqueURI());
        RepositoryConnection connection = mock(RepositoryConnection.class);
        when(dataUnit.getConnection()).thenReturn(connection);
        DataUnitRDFWriterWithMetadata writer = new DataUnitRDFWriterWithMetadata(dataUnit);

        // Act
        writer.close();

        // Assert
        verify(connection).close();
    }

    @Test
    public void addNamespaceDoesNotThrow() throws Exception {
        writer.addNamespace("prefix", "url");
    }
}