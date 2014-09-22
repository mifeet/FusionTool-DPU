package eu.unifiedviews.plugins.transformer.fusiontool.io;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolvedStatementImpl;
import eu.unifiedviews.dataunit.rdf.WritableRDFDataUnit;
import eu.unifiedviews.plugins.transformer.fusiontool.config.FTConfigConstants;
import eu.unifiedviews.plugins.transformer.fusiontool.testutils.FTDPUTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

public class DataUnitRDFWriterTest {

    public static final URI DEFAULT_CONTEXT = FTDPUTestUtils.getUniqueURI();
    private DataUnitRDFWriter writer;
    private Repository repository;
    private RepositoryConnection connection;

    @Before
    public void setUp() throws Exception {
        repository = FTDPUTestUtils.createRepository(ImmutableList.<Statement>of());
        WritableRDFDataUnit dataUnit = mock(WritableRDFDataUnit.class);
        when(dataUnit.getBaseDataGraphURI()).thenReturn(DEFAULT_CONTEXT);
        connection = repository.getConnection();
        when(dataUnit.getConnection()).thenReturn(connection);
        when(dataUnit.addNewDataGraph(FTConfigConstants.DEFAULT_DATA_GRAPH_NAME)).thenReturn(DEFAULT_CONTEXT);
        writer = new DataUnitRDFWriter(dataUnit, FTConfigConstants.DEFAULT_DATA_GRAPH_NAME);
    }

    @After
    public void tearDown() throws Exception {
        connection.close();
        repository.shutDown();
    }

    @Test
    public void writesStatementToDefaultGraph() throws Exception {
        Statement statement = FTDPUTestUtils.createStatement();

        writer.write(statement);

        Statement actualStatement = Iterables.getOnlyElement(FTDPUTestUtils.getAllStatements(repository));
        assertThat(actualStatement, is(statement));
        assertThat(actualStatement.getContext(), is((Resource) DEFAULT_CONTEXT));
    }

    @Test
    public void writesResolvedStatementToDefaultGraph() throws Exception {
        Statement statement = FTDPUTestUtils.createStatement();
        ResolvedStatement resolvedStatement = new ResolvedStatementImpl(statement, 0.5, ImmutableList.of((Resource) FTDPUTestUtils.getUniqueURI()));

        writer.write(resolvedStatement);

        Statement actualStatement = Iterables.getOnlyElement(FTDPUTestUtils.getAllStatements(repository));
        assertThat(actualStatement, is(statement));
        assertThat(actualStatement.getContext(), is((Resource) DEFAULT_CONTEXT));
    }

    @Test
    public void closesConnection() throws Exception {
        // Arrange
        WritableRDFDataUnit dataUnit = mock(WritableRDFDataUnit.class);
        when(dataUnit.getBaseDataGraphURI()).thenReturn(DEFAULT_CONTEXT);
        RepositoryConnection connection = mock(RepositoryConnection.class);
        when(dataUnit.getConnection()).thenReturn(connection);
        writer = new DataUnitRDFWriter(dataUnit, FTConfigConstants.DEFAULT_DATA_GRAPH_NAME);

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