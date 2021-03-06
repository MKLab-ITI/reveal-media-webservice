package gr.iti.mklab.reveal.summarization;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.LowerCaseTokenizer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.position.PositionFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;

@SuppressWarnings("deprecation")
public class TextAnalyser {

	private static Analyzer stdAnalyzer = new StandardAnalyzer();
		 
	public static List<String> getTokens(String text) {
		
		List<String> tokens = new ArrayList<String>();
		
		TokenStream ts = null;
		try {
			ts = stdAnalyzer.tokenStream("text", text);
			CharTermAttribute charTermAtt = ts.addAttribute(CharTermAttribute.class);
			
			ts.reset();
			while (ts.incrementToken()) {
				String token = charTermAtt.toString();
				if(token.length() > 1) {
					tokens.add(token);
				}
		      }

		} catch (Exception e) {
			
		}
		finally {
			if(ts != null) {
			      try {
					ts.end();
					ts.close();
				} catch (IOException e) {
					e.printStackTrace();
				}  
			}
		}
		
		return tokens;
	}
    
	public static void getTokens(String text, List<String> tokens) {
		tokens.addAll(getTokens(text));
	}

	public static List<String> getNgrams(String text) throws IOException {
		return getNgrams(text, 1);
	}
	
	public static List<String> getNgrams(String text, int N) throws IOException {
		
		List<String> tokens = new ArrayList<String>();
		if(text == null) {
			return tokens;
		}
		
		Reader reader = new StringReader(text);
		LowerCaseTokenizer tokenizer = new LowerCaseTokenizer(reader);
		
		// Filters
		LowerCaseFilter lowerCaseFilter = new LowerCaseFilter(tokenizer); 
		KStemFilter kStemFilter = new KStemFilter(lowerCaseFilter);
		
		CharArraySet stopwords = StopAnalyzer.ENGLISH_STOP_WORDS_SET;
		StopFilter stopFilter = new StopFilter(kStemFilter, stopwords);

		TokenStream ts;
		if(N > 1) {
			PositionFilter positionFilter = new PositionFilter(stopFilter);
			
			@SuppressWarnings("resource")
			ShingleFilter shingleFilter = new ShingleFilter(positionFilter, 2, N);
			shingleFilter.setOutputUnigrams(true);
			
			ts = shingleFilter;
		}
		else {
			ts = stopFilter;
		}
		
		CharTermAttribute charTermAtt = ts.addAttribute(CharTermAttribute.class);
		
		ts.reset();
		while (ts.incrementToken()) {
			String token = charTermAtt.toString();
			if(token.length() > 1) {
				tokens.add(token);
			}
		}
		ts.end();  
		ts.close();
	      
		return tokens;
	}

}
