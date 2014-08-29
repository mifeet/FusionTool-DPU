package eu.unifiedviews.plugins.transformer.fusiontool.util;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.AlternativeUriNavigator;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterableImpl;
import eu.unifiedviews.dataunit.rdf.RDFDataUnit;
import eu.unifiedviews.plugins.transformer.fusiontool.testutils.FTDPUTestUtils;
import eu.unifiedviews.plugins.transformer.fusiontool.testutils.MockIteration;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.memory.MemoryStore;

import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QuadLoaderTest {
    ValueFactory VF = ValueFactoryImpl.getInstance();

    @Test
    public void loadsQuadsWithoutAlternativeUris() throws Exception {
        // Arrange
        Repository repository = new SailRepository(new MemoryStore());
        repository.initialize();
        repository.getConnection().add(FTDPUTestUtils.createHttpStatement("abc", "p", "o1", "graph"));
        repository.getConnection().add(FTDPUTestUtils.createHttpStatement("abc2", "p", "o2", "graph"));
        repository.getConnection().add(FTDPUTestUtils.createHttpStatement("abc", "p", "o3", "graph1"));
        RDFDataUnit.class.getName();
        RDFDataUnit rdfDataUnit = mock(RDFDataUnit.class);
        when(rdfDataUnit.getIteration()).thenReturn(new MockIteration("http://graph"));
        when(rdfDataUnit.getConnection()).thenReturn(repository.getConnection());

        QuadLoader quadLoader = new QuadLoader(Collections.singletonList(rdfDataUnit),
                new AlternativeUriNavigator(new UriMappingIterableImpl()),
                new PrefixDeclBuilderImpl(Collections.<String, String>emptyMap()));

        // Act
        Collection<Statement> statements = quadLoader.loadQuadsForURI(FTDPUTestUtils.createHttpUri("abc").stringValue());

        // Assert
        assertThat(statements, contains(FTDPUTestUtils.createHttpStatement("abc", "p", "o1", "graph")));
    }

    @Test
    public void loadsQuadsWithAlternativeUris() throws Exception {
        // Arrange
        Repository repository = new SailRepository(new MemoryStore());
        repository.initialize();
        repository.getConnection().add(FTDPUTestUtils.createHttpStatement("abc", "p", "o1", "graph"));
        repository.getConnection().add(FTDPUTestUtils.createHttpStatement("abc_sa", "p", "o4", "graph"));
        repository.getConnection().add(FTDPUTestUtils.createHttpStatement("abc2", "p", "o2", "graph"));
        repository.getConnection().add(FTDPUTestUtils.createHttpStatement("abc", "p", "o3", "graph1"));
        RDFDataUnit.class.getName();
        RDFDataUnit rdfDataUnit = mock(RDFDataUnit.class);
        when(rdfDataUnit.getIteration()).thenReturn(new MockIteration("http://graph"));
        when(rdfDataUnit.getConnection()).thenReturn(repository.getConnection());

        UriMappingIterableImpl uriMapping = new UriMappingIterableImpl();
        uriMapping.addLink(FTDPUTestUtils.createHttpUri("abc").stringValue(), FTDPUTestUtils.createHttpUri("abc_sa").stringValue());
        QuadLoader quadLoader = new QuadLoader(Collections.singletonList(rdfDataUnit),
                new AlternativeUriNavigator(uriMapping),
                new PrefixDeclBuilderImpl(Collections.<String, String>emptyMap()));

        // Act
        Collection<Statement> statements = quadLoader.loadQuadsForURI(FTDPUTestUtils.createHttpUri("abc").stringValue());

        // Assert
        assertThat(statements, contains(
                FTDPUTestUtils.createHttpStatement("abc", "p", "o1", "graph"),
                FTDPUTestUtils.createHttpStatement("abc_sa", "p", "o4", "graph")));
    }
}