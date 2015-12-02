package gr.iti.mklab.reveal.crawler.seeds;

import java.util.Set;

/**
 * Interface to be implemented by all classes that produce seed URLs for the crawler
 * 
 * @author kandreads
 *
 */
public interface SeedURLSource {

	public Set<String> getSeedURLs(Set<String> keywords);
}
