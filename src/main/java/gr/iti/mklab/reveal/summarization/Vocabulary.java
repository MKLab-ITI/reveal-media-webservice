package gr.iti.mklab.reveal.summarization;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import gr.iti.mklab.reveal.summarization.utils.VectorUtils;


public class Vocabulary {

	private static Integer docs = 0;
	private static Map<String, Integer> map = new HashMap<String, Integer>();
	
	private static Set<String> boostedTerms = new HashSet<String>();
	public static double boost = 2;
	
	private static Collection<String> stopwords = null;
	
	public static Map<String, Vector> createVocabulary(Map<String, String> texts) {
		return createVocabulary(texts, 1);
	}
	
	public static Map<String, Vector> createVocabulary(Map<String, String> texts, int ngrams) {
		Vocabulary.reset();
		Map<String, Vector> vectors = new HashMap<String, Vector>();

		for(Entry<String, String> entry : texts.entrySet()) {
			
			String id = entry.getKey();
			String text = entry.getValue();
			Vector vector = process(text, ngrams);
			if(vector != null) {
				vectors.put(id, vector);
			}
		}
		
		Collection<String> stowords = Vocabulary.getStopwords();
		Vocabulary.removeWords(stowords);
		
		return vectors;
	}
	
	public static Map<String, Vector> updateVocabulary(Map<String, String> texts, int ngrams) {
		Map<String, Vector> vectors = new HashMap<String, Vector>();
		for(Entry<String, String> entry : texts.entrySet()) {
			String id = entry.getKey();
			String text = entry.getValue();
			Vector vector = process(text, ngrams);
			if(vector != null) {
				vectors.put(id, vector);
			}
		}
		
		Collection<String> stowords = Vocabulary.getStopwords();
		Vocabulary.removeWords(stowords);
		
		return vectors;
	}
	
	public static Map<String, Double[]> createVocabulary(Map<String, String> texts, int ngrams, int n) {
		
		Map<String, Double[]> tVectors = new HashMap<String, Double[]>();
		Map<String, Vector> vectors = createVocabulary(texts, ngrams);
		
		System.out.println(Vocabulary.getTerms().size() + " terms");
		
		List<String> terms = getTopTerms(n);
		n = terms.size();
		
		for(String id : vectors.keySet()) {
			Vector vector = vectors.get(id);
			
			Double[] v = null;
			if(vector.getTerms().isEmpty()) {
				v = VectorUtils.getRandomVector(n);
			}
			else {
				v = new Double[n];
				Arrays.fill(v, .0);
				for(String term : vector.getTerms()) {
					int i = terms.indexOf(term);
					if(i >= 0 && i < n) {
						v[i] = vector.getTfIdf(term);
					}
				}
			}
			tVectors.put(id, v);
		}
		
		return tVectors;
	}
	
	
	public static void create(List<String> texts) {
		for(String text : texts) {
			
			List<String> tokens;
			try {
				tokens = TextAnalyser.getTokens(text);
			} catch (Exception e) {
				continue;
			}
			addDoc(tokens);
		}
	}
	
	private static Vector process(String text, int ngrams) {

		try {
			Set<String> tokens = new HashSet<String>();
			
			List<String> ngramsList = TextAnalyser.getNgrams(text, ngrams);
			tokens.addAll(ngramsList);
			
			addDoc(tokens);
			
			Vector vector = new Vector(tokens);
			return vector;
		} catch (IOException e) {
			return null;
		}

	}
	
	public static void addDoc(Collection<String> words) {
		if(words == null || words.isEmpty()) {
			return;
		}
		
		docs++;
		Set<String> tokens = new HashSet<String>(words);
		for(String word : tokens) {
			word = word.replaceAll("\\s+", " ").trim();
			
			Integer df = map.get(word);
			if(df == null) {
				df = 0;
			}
			map.put(word, ++df);
		}
	}
	
	public static Double getDf(String word) {
		word = word.replaceAll("\\s+", " ").trim();
		Integer df = map.get(word);
		if(df == null || docs == 0) {
			return 0d;
		}
		return df.doubleValue() / docs.doubleValue();
	}
	
	public static Double getIdf(String word) {
		word = word.replaceAll("\\s+", " ").trim();
		Integer df = map.get(word);
		if(df == null) {
			return 0d;
		}
		return Math.log(docs.doubleValue()/df.doubleValue());
	}
	
	public static Integer getIndex(String word) {
		word = word.replaceAll("\\s+", " ").trim();
		Set<String> keys = map.keySet();
		List<String> list = new ArrayList<String>(keys);
		
		return list.indexOf(word);
	}
	
	public static Double getBoost(String word) {
		word = word.replaceAll("\\s+", " ").trim();
		if(boostedTerms.contains(word)) {
			return boost;
		}
		else {
			return 1d;
		}
	}
	
	public static Integer getNumOfDocs() {
		return docs;
	}
	
	public static Integer getNumOfTerms() {
		return map.size();
	}
	
	public static Set<String> getTerms() {
		Set<String> terms = new HashSet<String>();
		terms.addAll(map.keySet());
		
		return terms;
	}
	
	public static List<String> getTopTerms(int n) {
		List<String> terms = new ArrayList<String>();
		terms.addAll(map.keySet());

		Collections.sort(terms, new Comparator<String>() {
			@Override
			public int compare(String t1, String t2) {
				Double v1 = Vocabulary.getDf(t1);
				Double v2 = Vocabulary.getDf(t2);
				
				if(v1 == v2)
					return 0;
				else
					return v1 > v2 ? 1 : -1;
			}
		});
		
		return terms.subList(0, Math.min(n, terms.size()-1));
	}
	
	public static Set<String> getBoostedTerms() {
		Set<String> terms = new HashSet<String>();
		terms.addAll(Vocabulary.boostedTerms);
	
		return terms;
	}
	
	public static void print() {
		System.out.println(docs + " documents");
		System.out.println(map.size() + " tokens");
	}
	
	public static Collection<String> getStopwords() {
		if(stopwords == null) {
			double minDF = 10.0/docs.doubleValue();
			stopwords = new HashSet<String>();
			for(String word : map.keySet()) {
				Double df = getDf(word);		
				if(df < minDF || df > 0.6) {
					stopwords.add(word);
				}
			}
		}
		return stopwords;
	}

	public static void addBoostedTerms(Collection<String> terms) {
		for(String term : terms) {
			term = term.replaceAll("\\s+", " ").trim();
			boostedTerms.add(term);
		}
	}

	public static void removeWords(Collection<String> words) {
		for(String word : words) {
			map.remove(word);
		}
	}

	public synchronized static void reset() {
		docs = 0;
		map.clear();
		boostedTerms.clear();
	}
}
