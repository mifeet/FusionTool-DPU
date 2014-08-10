package eu.unifiedviews.plugins.transformer.fusiontool.util;

/**
 * Queue with URIs.
 * @author Jan Michelfeit
 */
public interface UriQueue  {
    /**
     * Returns {@code true} if the collection has more elements.
     * @return {@code true} if the collection has more elements
     */
    boolean hasNext();

    /**
     * Returns an element from the collection (in no particular order) and removes it.
     * @return the removed element
     */
    String next();
    
    /**
     * Adds a new URI to the collection. 
     * @param uri the uri to add
     */
    void add(String uri);
}
