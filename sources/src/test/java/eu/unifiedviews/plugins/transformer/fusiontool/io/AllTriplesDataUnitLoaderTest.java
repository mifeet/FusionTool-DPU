package eu.unifiedviews.plugins.transformer.fusiontool.io;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.rdf.RDFDataUnit;
import eu.unifiedviews.plugins.transformer.fusiontool.testutils.FTDPUTestUtils;
import eu.unifiedviews.plugins.transformer.fusiontool.util.MockRDFDataUnit;
import org.junit.Test;
import org.mockito.Mockito;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.helpers.StatementCollector;

import java.util.Collection;
import java.util.HashSet;

import static eu.unifiedviews.plugins.transformer.fusiontool.testutils.FTDPUTestUtils.createHttpStatement;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class AllTriplesDataUnitLoaderTest {

    @Test
    public void loadsAllTriplesWhenNumberOfStatementsIsNotDivisibleByMaxResultSize() throws Exception {
        // Arrange
        Collection<Statement> statements = ImmutableSet.of(
                createHttpStatement("s1", "p", "o", "g1"),
                createHttpStatement("s2", "p", "o", "g2"),
                createHttpStatement("s3", "p", "o", "g3"),
                createHttpStatement("s4", "p", "o", "g4"),
                createHttpStatement("s5", "p", "o", "g5")
        );

        // Act
        Collection<Statement> result = new HashSet<>();
        try (MockRDFDataUnit rdfDataUnit = getDataUnit(statements);
             AllTriplesDataUnitLoader loader = getLoader(rdfDataUnit, 2)
        ) {
            loader.loadAllTriples(new StatementCollector(result));
        }

        // Assert
        assertThat(result, is(statements));
    }

    @Test
    public void loadsAllTriplesWhenNumberOfStatementsIsDivisibleByMaxResultSize() throws Exception {
        // Arrange
        Collection<Statement> statements = ImmutableSet.of(
                createHttpStatement("s1", "p", "o", "g1"),
                createHttpStatement("s2", "p", "o", "g2"),
                createHttpStatement("s3", "p", "o", "g3"),
                createHttpStatement("s4", "p", "o", "g4")
        );
        Repository repository = FTDPUTestUtils.createRepository(statements);

        // Act
        Collection<Statement> result = new HashSet<>();
        try (MockRDFDataUnit rdfDataUnit = getDataUnit(statements);
             AllTriplesDataUnitLoader loader = getLoader(rdfDataUnit, 2)
        ) {
            loader.loadAllTriples(new StatementCollector(result));
        }

        // Assert
        assertThat(result, is(statements));
    }

    @Test
    public void loadGraphsGivenByIterationOnly() throws Exception {
        // Arrange
        Collection<Statement> statements = ImmutableSet.of(
                createHttpStatement("s1", "p", "o", "g1"),
                createHttpStatement("s2", "p", "o", "g2")
        );
        Collection<Statement> extraStatements = ImmutableSet.of(
                createHttpStatement("s3", "p", "o", "g3"),
                createHttpStatement("s4", "p", "o", "g4")
        );
        Repository repository = FTDPUTestUtils.createRepository(statements);

        // Add extra statements after loader is created and graphs configured
        RepositoryConnection connection = repository.getConnection();
        connection.add(extraStatements);
        connection.close();

        // Act
        Collection <Statement> result = new HashSet<>();
        try (MockRDFDataUnit rdfDataUnit = getDataUnit(statements);
             AllTriplesDataUnitLoader loader = getLoader(rdfDataUnit, 2)
        ) {
            loader.loadAllTriples(new StatementCollector(result));
        }

        // Assert
        assertThat(result, is(statements));
    }

    @Test
    public void returnsEmptyResultWhenNoMatchingTriplesExist() throws Exception {
        // Arrange
        Collection<Statement> statements = ImmutableList.of();
        Repository repository = FTDPUTestUtils.createRepository(statements);

        // Act
        Collection<Statement> result = new HashSet<>();
        try (MockRDFDataUnit rdfDataUnit = getDataUnit(statements);
             AllTriplesDataUnitLoader loader = getLoader(rdfDataUnit, 2)
        ) {
            loader.loadAllTriples(new StatementCollector(result));
        }

        // Assert
        assertThat(result, empty());
    }

    @Test
    public void callsStartRDFAndEndRDFOnGivenHandler() throws Exception {
        // Arrange
        RDFHandler rdfHandler = mock(RDFHandler.class);
        Collection<Statement> statements = ImmutableList.of(
                createHttpStatement("s1", "p", "o", "g1")
        );
        Repository repository = FTDPUTestUtils.createRepository(statements);

        // Act
        try (MockRDFDataUnit rdfDataUnit = getDataUnit(statements);
             AllTriplesDataUnitLoader loader = getLoader(rdfDataUnit, 2)
        ) {
            loader.loadAllTriples(rdfHandler);
        }

        // Assert
        Mockito.verify(rdfHandler).startRDF();
        Mockito.verify(rdfHandler).endRDF();
    }

    @Test
    public void returnsNonNullDefaultContext() throws Exception {
        // Arrange
        Collection<Statement> statements = ImmutableList.of(
                createHttpStatement("s1", "p", "o", "g1")
        );
        Repository repository = FTDPUTestUtils.createRepository(statements);

        // Act
        URI defaultContext;
        try (MockRDFDataUnit rdfDataUnit = getDataUnit(statements);
             AllTriplesDataUnitLoader loader = getLoader(rdfDataUnit, 2)
        ) {
            defaultContext = loader.getDefaultContext();
        }

        // Assert
        assertTrue(ODCSUtils.isValidIRI(defaultContext.stringValue()));
    }

    private AllTriplesDataUnitLoader getLoader(RDFDataUnit rdfDataUnit, int maxSparqlResultsSize) throws DataUnitException {
        AllTriplesDataUnitLoader loader = new AllTriplesDataUnitLoader(rdfDataUnit);
        loader.setMaxSparqlResultsSize(maxSparqlResultsSize);
        return loader;
    }

    private MockRDFDataUnit getDataUnit(Collection<Statement> statements) throws RepositoryException, DataUnitException {
        MockRDFDataUnit rdfDataUnit = new MockRDFDataUnit(statements);
        for (Statement statement : statements) {
            URI graphName = (URI) statement.getContext();
            rdfDataUnit.addExistingDataGraph(graphName.stringValue(), graphName);
        }
        return rdfDataUnit;
    }
}