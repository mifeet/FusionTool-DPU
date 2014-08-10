package eu.unifiedviews.plugins.transformer.fusiontool.util;

import java.util.Iterator;
import java.util.Set;

import eu.unifiedviews.plugins.transformer.fusiontool.io.LargeCollectionFactory;

/**
 * Queue with triple subjects discovered during traversing of triples.
 * The collection behaves like a Set for addition of new URIs.
 * The class DOES NOT keep track of nodes that were popped from the queue.
 * Nodes other than resource URIs or blank nodes are discarded.
 * @author Jan Michelfeit
 */
public class UriQueueImpl implements UriQueue {
    private final Set<String> uriQueue;

    /**
     * Creates a new instance.
     * @param collectionFactory factory for the underlying collection 
     */
    public UriQueueImpl(LargeCollectionFactory collectionFactory) {
        uriQueue = collectionFactory.createSet();
    }
    
    @Override
    public boolean hasNext() {
        return !uriQueue.isEmpty();
    }

    @Override
    public String next() {
        Iterator<String> it = uriQueue.iterator();
        String result = it.next();
        it.remove();
        return result;
    }

    @Override
    public void add(String uri) {
        uriQueue.add(uri);
    }
    
    /**
     * Returns size of the collection.
     * @return number of contained elements
     */
    public int size() {
        return uriQueue.size();
    }
}
