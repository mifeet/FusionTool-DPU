package eu.unifiedviews.plugins.transformer.fusiontool.io;

import java.io.IOException;
import java.util.Set;

/**
 * Factory interface for potentially large collections. 
 * @author Jan Michelfeit
 */
public interface LargeCollectionFactory  {
    /**
     * Creates a new {@link Set}.
     * @param <T> type of collection
     * @return a new Set
     */
    <T> Set<T> createSet();
    
    /**
     * Releases any resources associated with this object.
     * If the object is already closed then invoking this method has no effect.
     * @throws IOException exception
     */
    void close() throws IOException;
}

