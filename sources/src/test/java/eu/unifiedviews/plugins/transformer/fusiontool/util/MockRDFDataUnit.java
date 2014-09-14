package eu.unifiedviews.plugins.transformer.fusiontool.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.rdf.RDFDataUnit;
import eu.unifiedviews.dataunit.rdf.WritableRDFDataUnit;
import eu.unifiedviews.plugins.transformer.fusiontool.testutils.FTDPUTestUtils;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MockRDFDataUnit implements WritableRDFDataUnit, AutoCloseable {
    private final Repository repository;
    private URI metadataGraphURI = FTDPUTestUtils.getUniqueURI();
    private URI dataGraphURI = FTDPUTestUtils.getUniqueURI();
    private Map<String, URI> addedGraphs = new HashMap<>();

    public MockRDFDataUnit(Repository repository) {
        this.repository = repository;
    }

    @Override
    public RepositoryConnection getConnection() throws DataUnitException {
        try {
            return repository.getConnection();
        } catch (RepositoryException e) {
            throw new DataUnitException(e);
        }
    }

    @Override
    public Set<URI> getMetadataGraphnames() throws DataUnitException {
        return ImmutableSet.of(metadataGraphURI);
    }

    @Override
    public RDFDataUnit.Iteration getIteration() throws DataUnitException {
        return new MockIteration(dataGraphURI);
    }

    @Override
    public void close() throws Exception {
        repository.shutDown();
    }

    @Override
    public URI getBaseDataGraphURI() throws DataUnitException {
        return dataGraphURI;
    }

    @Override
    public void addExistingDataGraph(String s, URI uri) throws DataUnitException {
        addedGraphs.put(s, uri);
    }

    @Override
    public URI addNewDataGraph(String s) throws DataUnitException {
        URI uri = FTDPUTestUtils.createHttpUri(s);
        addedGraphs.put(s, uri);
        return uri;
    }

    @Override
    public void addEntry(String s) throws DataUnitException {

    }

    @Override
    public URI getMetadataWriteGraphname() throws DataUnitException {
        return metadataGraphURI;
    }

    public Map<String, URI> getAddedGraphs() {
        return addedGraphs;
    }

    public List<Statement> getAllStatements() throws Exception {
        return FTDPUTestUtils.getAllStatements(repository);
    }

    private static class MockIteration implements RDFDataUnit.Iteration {
        private boolean isFirst = true;
        private final URI dataGraphURI;

        private MockIteration(URI dataGraphURI) {
            this.dataGraphURI = dataGraphURI;
        }

        @Override
        public boolean hasNext() throws DataUnitException {
            return isFirst;
        }

        @Override
        public RDFDataUnit.Entry next() throws DataUnitException {
            Preconditions.checkState(isFirst);
            isFirst = false;
            return new MockEntry(dataGraphURI);
        }

        @Override
        public void close() throws DataUnitException {

        }
    }

    private static class MockEntry implements RDFDataUnit.Entry {
        private final URI dataGraphURI;

        private MockEntry(URI dataGraphURI) {
            this.dataGraphURI = dataGraphURI;
        }

        @Override
        public URI getDataGraphURI() throws DataUnitException {
            return dataGraphURI;
        }

        @Override
        public String getSymbolicName() throws DataUnitException {
            return dataGraphURI.stringValue();
        }
    }
}
