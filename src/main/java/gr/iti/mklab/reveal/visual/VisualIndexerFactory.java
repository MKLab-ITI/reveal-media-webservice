package gr.iti.mklab.reveal.visual;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A factory for VisualIndexers, which holds a cache of instances that
 * expire after one hour of not being accessed.
 *
 * @author kandreadou
 *
 */
public class VisualIndexerFactory {

    private static LoadingCache<String, VisualIndexer> INDEXERS_CACHE = CacheBuilder.newBuilder()
            .expireAfterAccess(1, TimeUnit.HOURS)
            .build(
                    new CacheLoader<String, VisualIndexer>() {
                        public VisualIndexer load(String collection) throws Exception {
                            return new VisualIndexer(collection);
                        }
                    });


    public static VisualIndexer getVisualIndexer(String collection) throws ExecutionException {
        return INDEXERS_CACHE.get(collection);
    }
}
