package gr.iti.mklab.reveal.clustering;

import com.aliasi.tokenizer.ModifyTokenTokenizerFactory;
import com.aliasi.tokenizer.RegExTokenizerFactory;
import com.aliasi.tokenizer.Tokenizer;
import com.aliasi.tokenizer.TokenizerFactory;

/**
 * The goal of this class is to perform tweet deduplication via normalization, which
 * means to just keep "unique" tweets and discard near duplicates, for instance tweets
 * only differing in the url for bit.ly or retweets which start with RT@. This tokenizer
 * removes non characteristic tokens like "RT" and "@" in order to make tweets more uniform
 * by eliminating trivial differences.
 *
 * @author kandreadou
 */
public class NormalizedTokenizerFactory extends ModifyTokenTokenizerFactory {


    public NormalizedTokenizerFactory(TokenizerFactory factory) {
        super(factory);
    }

    public NormalizedTokenizerFactory() {
        super(new RegExTokenizerFactory("http:[^\\s]*|@[^\\s]*|\\w+"));
    }

    public String modifyToken(String token) {
        if (token.matches("RT")) {
            return null;
        }
        else if (token.startsWith("@")) {
            return null;
        }
        else if (token.startsWith("http")) {
            return null;
        }

        else {
            return token;
        }
    }

    public static void main(String[] args) {
        TokenizerFactory tokFactory = new NormalizedTokenizerFactory();
        String text = "RT @pimpmytweeting This is what Foreign Aid is ALL about.... not palatial mansions and rockets.... #NepalEarthquake http://t.co/psgFuohclB";
        System.out.println("Tweet: " + text);
        char[] chars = text.toCharArray();
        Tokenizer tokenizer
                = tokFactory.tokenizer(chars,0,chars.length);
        String token;
        System.out.println("White Space :'" +  tokenizer.nextWhitespace() + "'");
        while ((token = tokenizer.nextToken()) != null) {
            System.out.println("Token: " + token);
            System.out.println("White Space :'" + tokenizer.nextWhitespace()+"'");
        }
    }
}