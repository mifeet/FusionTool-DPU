package eu.unifiedviews.plugins.transformer.fusiontool.testutils;

import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.rdf.RDFDataUnit;
import org.openrdf.model.URI;

/**
*
*/
public class MockEntry implements RDFDataUnit.Entry {
    private final URI uri;

    public MockEntry(URI uri) {
        this.uri = uri;
    }

    @Override
    public URI getDataGraphURI() throws DataUnitException {
        return uri;
    }

    @Override
    public String getSymbolicName() throws DataUnitException {
        return uri.stringValue();
    }
}
