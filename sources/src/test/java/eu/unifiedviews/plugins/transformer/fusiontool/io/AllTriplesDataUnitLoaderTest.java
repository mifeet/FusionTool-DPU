package eu.unifiedviews.plugins.transformer.fusiontool.io;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.xrg.odcs.commons.app.dataunit.rdf.AbstractRDFDataUnit;
import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.plugins.transformer.fusiontool.testutils.FTDPUTestUtils;
import org.junit.Test;
import org.mockito.Mockito;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.sail.memory.MemoryStore;

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
        Repository repository = createRepository(statements);

        // Act
        Collection<Statement> result = new HashSet<>();
        AllTriplesDataUnitLoader loader = createLoader(repository, 2);
        try {
            loader.loadAllTriples(new StatementCollector(result));
        } finally {
            loader.close();
            repository.shutDown();
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
        Repository repository = createRepository(statements);

        // Act
        Collection<Statement> result = new HashSet<>();
        AllTriplesDataUnitLoader loader = createLoader(repository, 2);
        try {
            loader.loadAllTriples(new StatementCollector(result));
        } finally {
            loader.close();
            repository.shutDown();
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
        Repository repository = createRepository(statements);
        AllTriplesDataUnitLoader loader = createLoader(repository, 2);

        // Add extra statements after loader is created and graphs configured
        RepositoryConnection connection = repository.getConnection();
        connection.add(extraStatements);
        connection.close();

        // Act
        Collection <Statement> result = new HashSet<>();
        try {
            loader.loadAllTriples(new StatementCollector(result));
        } finally {
            loader.close();
            repository.shutDown();
        }

        // Assert
        assertThat(result, is(statements));
    }

    @Test
    public void returnsEmptyResultWhenNoMatchingTriplesExist() throws Exception {
        // Arrange
        Collection<Statement> statements = ImmutableList.of();
        Repository repository = createRepository(statements);

        // Act
        Collection<Statement> result = new HashSet<>();
        AllTriplesDataUnitLoader loader = createLoader(repository, 2);
        try {
            loader.loadAllTriples(new StatementCollector(result));
        } finally {
            loader.close();
            repository.shutDown();
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
        Repository repository = createRepository(statements);

        // Act
        AllTriplesDataUnitLoader loader = createLoader(repository, 2);
        try {
            loader.loadAllTriples(rdfHandler);
        } finally {
            loader.close();
            repository.shutDown();
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
        Repository repository = createRepository(statements);

        // Act
        AllTriplesDataUnitLoader loader = createLoader(repository, 2);
        URI defaultContext;
        try {
            defaultContext = loader.getDefaultContext();
        } finally {
            loader.close();
            repository.shutDown();
        }

        // Assert
        assertTrue(ODCSUtils.isValidIRI(defaultContext.stringValue()));
    }

    private Repository createRepository(Collection<Statement> statements) throws RepositoryException {
        Repository repository = new SailRepository(new MemoryStore());
        repository.initialize();
        RepositoryConnection connection = repository.getConnection();
        connection.add(statements);
        connection.close();
        return repository;
    }

    private AllTriplesDataUnitLoader createLoader(Repository repository, int maxSparqlResultsSize) throws RepositoryException, DataUnitException {
        TestRDFDataUnit rdfInput = new TestRDFDataUnit(repository);
        AllTriplesDataUnitLoader loader = new AllTriplesDataUnitLoader(rdfInput);
        loader.setMaxSparqlResultsSize(maxSparqlResultsSize);
        return loader;
    }

    private static class TestRDFDataUnit extends AbstractRDFDataUnit {
        private final Repository repository;

        public TestRDFDataUnit(Repository repository) throws RepositoryException, DataUnitException {
            super("testDataUnit", FTDPUTestUtils.createHttpUri("writeContext").stringValue());
            this.repository = repository;
            RepositoryConnection connection = repository.getConnection();
            RepositoryResult<Statement> statements = connection.getStatements(null, null, null, false);
            while (statements.hasNext()) {
                URI graphName = (URI) statements.next().getContext();
                addExistingDataGraph(graphName.stringValue(), graphName);
            }
            statements.close();
            connection.close();
        }

        @Override
        public RepositoryConnection getConnectionInternal() throws RepositoryException {
            return repository.getConnection();
        }
    }
}