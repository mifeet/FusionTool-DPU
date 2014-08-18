package eu.unifiedviews.plugins.transformer.fusiontool.testutils;

import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.rdf.RDFDataUnit;
import org.openrdf.model.impl.ValueFactoryImpl;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
*
*/
public class MockIteration implements RDFDataUnit.Iteration {
    private final List<String> uris;
    private final Iterator<String> iterator;

    public MockIteration(String... uris) {
        this.uris = Arrays.asList(Arrays.copyOf(uris, uris.length));
        this.iterator = this.uris.iterator();
    }

    @Override
    public RDFDataUnit.Entry next() throws DataUnitException {
        return new MockEntry(ValueFactoryImpl.getInstance().createURI(iterator.next()));
    }

    @Override
    public boolean hasNext() throws DataUnitException {
        return iterator.hasNext();
    }

    @Override
    public void close() throws DataUnitException {
    }
}
