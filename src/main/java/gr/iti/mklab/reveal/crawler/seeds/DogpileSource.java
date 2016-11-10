package gr.iti.mklab.reveal.crawler.seeds;

import java.net.URI;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


public class DogpileSource implements SeedURLSource {

	public static int RESULTS_MAX = 100;

	@Override
	public Set<String> getSeedURLs(Set<String> keywords) {
		// The results list
		Set<String> results = new HashSet<>();
		// Form the query from the set of keywords
		String query = StringUtils.join(keywords, "+");
		for (int i = 1; i < RESULTS_MAX; i += 10) {
			try {
				// qsi= is used for getting more pages
				Document googleResults = Jsoup
						.connect(new URI("http://www.dogpile.com/search/web?q=" + query + "&qsi=" + i).toASCIIString())
						.userAgent("Mozilla/37.0").timeout(60000).get();
				Elements elements = googleResults.getElementById("webResults").select("div.resultDisplayUrlPane");
				// Extract the pure URL from the dogpile html structure
				for (Element el : elements) {
					try {
						String dogpileHref = el.select("a").attr("href");
						dogpileHref = URLDecoder.decode(dogpileHref, "UTF-8");
						String properLink = dogpileHref.substring(dogpileHref.indexOf("ru=") + 3, dogpileHref.lastIndexOf("&coi"));
						results.add(URLDecoder.decode(properLink, "UTF-8"));
					} catch (Exception ex) {
						System.out.println("Parsing exception " + ex);
					}

				}
			} catch (Exception ex) {
				System.out.println("IO exception " + ex);
				ex.printStackTrace();
			}
		}
		return results;
	}

	public static void main(String[] args) {
		Set<String> example = new HashSet<>();
		example.add("election");
		example.add("argentina");
		SeedURLSource instance = new DogpileSource();
		for (String result : instance.getSeedURLs(example)) {
			System.out.println(result);
		}
	}

}
