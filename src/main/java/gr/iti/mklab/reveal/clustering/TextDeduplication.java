package gr.iti.mklab.reveal.clustering;

import com.aliasi.spell.JaccardDistance;
import com.aliasi.tokenizer.Tokenizer;
import com.aliasi.tokenizer.TokenizerFactory;
import gr.iti.mklab.reveal.util.Configuration;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.items.Media;
import gr.iti.mklab.simmo.core.morphia.MediaDAO;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The goal of this class is given a set of tweets, to perform text deduplication
 * in order to only keep tweets which are sufficiently diverse in terms of their
 * lexical content
 *
 * @author kandreadou
 */
public class TextDeduplication {

    /**
     * This method uses the JaccardDistance and the NormalizedTokenizerFactory
     * to throw out tweets with too much overlap.
     *
     * @param media
     * @param tokFactory
     * @param cutoff
     * @param <T>
     * @return
     */
    public static <T extends Media> List<T> filterMediaJaccard(List<T> media,
                                                   TokenizerFactory tokFactory,
                                                   double cutoff) {
        JaccardDistance jaccardD = new JaccardDistance(tokFactory);
        List<T> filteredMedia = new ArrayList<>();
        for (int i = 0; i < media.size(); ++i) {
            String targetTweet = media.get(i).getTitle();
            boolean addTweet = true;
            //big research literature on making the below loop more efficient
            for (int j = 0; j < filteredMedia.size(); ++j ) {
                String comparisionTweet = filteredMedia.get(j).getTitle();
                double proximity
                        = jaccardD.proximity(targetTweet,comparisionTweet);
                if (proximity >= cutoff) {
                    addTweet = false;
                    break; //one nod to efficiency
                }
            }
            if (addTweet) {
                filteredMedia.add(media.get(i));
            }
        }
        return filteredMedia;
    }

    /**
     * This method uses the {@link gr.iti.mklab.reveal.clustering.NormalizedTokenizerFactory} to
     * filter out duplicates by normalizing the tweets
     *
     * @param media
     * @param tokFactory
     * @param <T>
     * @return
     */
    static <T extends Media> List<T> filterNormalizedDuplicates(List<T> media, TokenizerFactory tokFactory) {
        List<T> returnList = new ArrayList<>();
        Set<String> seenBefore = new HashSet<String>();
        for (int i = 0; i < media.size(); ++i) {
            String rawTweet = media.get(i).getTitle();
            String normalizedTweet
                    = buildStringFromTokensRemoveSeparators(tokFactory, rawTweet);
            if (!seenBefore.contains(normalizedTweet)) {
                returnList.add(media.get(i)); //this is what we will process
                seenBefore.add(normalizedTweet); //keeping track of normalized entries
            }
        }
        return returnList;
    }

    static String buildStringFromTokensRemoveSeparators(TokenizerFactory factory,
                                                        String tweet) {
        char[] chars = tweet.toCharArray();
        Tokenizer tokenizer
                = factory.tokenizer(tweet.toCharArray(), 0, chars.length);
        String token = null;
        StringBuilder sb = new StringBuilder();
        while ((token = tokenizer.nextToken()) != null) {
            sb.append(token);
        }
        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        TokenizerFactory tokFactory= new NormalizedTokenizerFactory();
        /*List<String> list = new ArrayList<>();
        list.add("RT @rakeshdas_11: Sad to see this.. Salute to volunteers #NepalDisasterReliefByMSG #NepalEarthquake @derasachasauda @Gurmeetramrahim https… ");
        list.add("RT @rakeshdas_11: Sad to see this.. Salute to volunteers #NepalDisasterReliefByMSG #NepalEarthquake @derasachasauda @Gurmeetramrahim https… ");
        list.add("Watch Nepal Earthquake Captured On A Hotel Pool Camera From Start Till The End http://t.co/JAwIVIh2Xx http://t.co/CT9qDd6sXo ");
        list.add("WATCH: This CCTV footage of a shaking swimming pool during earthquake in Nepal will give you goosebumps http://t.co/Sn0kivdA05 ");
        list.add("Nepal earth quick swimming pool CCTV EXCLUSIVE Footage http://t.co/AWjyIT13gJ");
        list.add("Viral Swimming Pool Video Shows 2010 Mexico Earthquake Not Nepal ... Nepal earthquake… http://t.co/O1XJbDOzTb http://t.co/oTc9SBEmn3");
        list.add("Nepal earth quick swimming pool CCTV EXCLUSIVE Footage http://t.co/Kgx1BiwtJU");
        //String text = "RT @mparent77772: Should Obama's 'internet kill switch' power be curbed? http://bbc.in/hcVGoz";
        //processAndWrite(list,tokFactory);
        List<String> filtered = filterTweetsJaccard(list, tokFactory, 0.7);
        filtered.stream().forEach(s->System.out.println(s));*/

        Configuration.load("remote.properties");
        MorphiaManager.setup("160.40.51.20");
        MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, "eurogroup");
        List<Image> imgs = new ArrayList<>();
        imgs.add(imageDAO.get("Twitter#614134890360123392"));
        imgs.add(imageDAO.get("Twitter#614134814493556736"));
        imgs.add(imageDAO.get("Twitter#614134809053503488"));
        imgs.add(imageDAO.get("Twitter#614135017774678018"));
        imgs.add(imageDAO.get("Twitter#614135025337024516"));
        imgs.add(imageDAO.get("Twitter#614135023801909250"));
        imgs.add(imageDAO.get("Twitter#614135023927721984"));
        imgs.add(imageDAO.get("Twitter#614135022350639104"));
        imgs.add(imageDAO.get("Twitter#614135023852220420"));
        imgs.add(imageDAO.get("Twitter#614135017560801280"));
        System.out.println("Filtering normalized");
        List<Image> filteredEasy = filterNormalizedDuplicates(imgs, tokFactory);
        filteredEasy.stream().forEach(s->System.out.println(s.getTitle()));
        System.out.println("Filtering using Jaccard distance");
        List<Image> filtered = filterMediaJaccard(filteredEasy, tokFactory, 0.7);
        filtered.stream().forEach(s->System.out.println(s.getTitle()));


    }

}
