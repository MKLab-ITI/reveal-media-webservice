package gr.iti.mklab.reveal.text;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author akrithara, gkatsios
 * @version 1.0
 * @since November 20, 2014
 */
public class TextPreprocessing {

    private ArrayList<String> ready = new ArrayList<String>();
    private String originalSentence = "";
    private String[] tokenizedText;

    /**
     * Constructs the class with the original Tweet text as parameter. This is a
     * pipeline to clean the Tweet so it can be ready for RE.
     *
     * @param tweetText
     */
    //public TextPreprocessing(String tweetText, ArrayList<String> atMentions, ArrayList<String> hashtags) {
    public TextPreprocessing(String tweetText) {
        originalSentence = tweetText;
        removeHTML();
        tokenizeText();
        cleanHead();
        cleanTail();
        cleanBody();
        //   swapSymbols(atMentions, hashtags);
        rebuildIt();
    }

    public TextPreprocessing(String tweetText, ArrayList<String> atMentions, ArrayList<String> hashtags) {
        originalSentence = tweetText;
        removeHTML();
        tokenizeText();
        cleanHead();
        cleanTail();
        cleanBody();
        swapSymbols(atMentions, hashtags);
        rebuildIt();
    }

    /**
     *
     * @return the ArrayList with the Cleaned Sentences, ready for NER and RE.
     */
    public ArrayList<String> getCleanedSentences() {
        return ready;
    }

    public void printArray() {
        /*ready.stream().forEach((s) -> {
            System.out.println(s);
        });*/
    }

    /**
     * Removes HTML leftovers.
     */
    private void removeHTML() {
        originalSentence = originalSentence.replaceAll("&amp;", "and");
        originalSentence = StringEscapeUtils.unescapeHtml4(originalSentence);
    }

    /**
     * Tokenizes the text and removes some noisy characters.
     */
    private void tokenizeText() {
        originalSentence = originalSentence.replaceAll("–", "-");
        originalSentence = originalSentence.replaceAll("!+", "!");
        originalSentence = originalSentence.replaceAll("\\?+", "?");
        originalSentence = originalSentence.replaceAll("…|\\.+", ".");
        originalSentence = originalSentence.replaceAll("(!\\?)+", "!?");
        originalSentence = originalSentence.replaceAll("(\\?!)+", "?!");
        originalSentence = originalSentence.replaceAll("\\r|\\n|\\r\\n", " ");
        originalSentence = originalSentence.replaceAll("\"|\\^|\\*|\\(|\\)|<|>|~|`|[^\\p{ASCII}]", "");
        tokenizedText = originalSentence.split("\\s+");
    }

    private void cleanBody() {
        for (int i = 0; i < tokenizedText.length; i++) {
            if (tokenizedText[i].startsWith("@")) {
                tokenizedText[i] = removeCamelCase(tokenizedText[i].replace("@", "")).trim();
            } else if (tokenizedText[i].startsWith("#")) {
                tokenizedText[i] = removeCamelCase(tokenizedText[i].replace("#", "")).trim();
            } else if (tokenizedText[i].startsWith("!") || tokenizedText[i].startsWith("?")) {
                tokenizedText[i] = tokenizedText[i].replaceAll("!+", "");
                tokenizedText[i] = tokenizedText[i].replaceAll("\\?+", "");
                if (i - 1 >= 0) {
                    tokenizedText[i - 1] = tokenizedText[i - 1] + ".";
                }
            }
        }
    }

    /**
     * Swaps At Mentions and Hashtags.
     */
    private void swapSymbols(ArrayList<String> atMentions, ArrayList<String> hashtags) {
        Iterator ith = hashtags.iterator();
        Iterator itm = atMentions.iterator();

        for (int i = 0; i < tokenizedText.length; i++) {
            if (tokenizedText[i].startsWith("@") && itm.hasNext()) {
                tokenizedText[i] = "@" + StringUtils.capitalize(itm.next().toString());
            } else if (tokenizedText[i].startsWith("#") && ith.hasNext()) {
                tokenizedText[i] = "#" + StringUtils.capitalize(ith.next().toString());
            }
        }
    }

    /**
     * Removes CamelCase. ("ILikePotatoes123" => "I Like Potatoes 123").
     */
    private String removeCamelCase(String str) {
        str = Arrays.toString(str.split("(?<!(^|\\p{Lu}))(?=\\p{Lu})|(?<!^)(?=\\p{Lu}\\p{Ll})"));
        str = Arrays.toString(str.split("(?<=[\\w&&\\D])(?=\\d)")).replaceAll("\\,|\\[|\\]", "");
        str = str.replaceAll("\\s+", " ");
        return str;
    }

    private void cleanHead() { //Removing from the start
        for (int i = 0; i < tokenizedText.length - 1; i++) {
            if (tokenizedText[i].startsWith("#")) {
                // tokenizedText[i] = "";
            } else if (tokenizedText[i].startsWith("@")) {
                //Don't touch
            } else if (tokenizedText[i].matches("(http)s?://.*")) {
                tokenizedText[i] = "";
            } else if (tokenizedText[i].matches("(RT)")) {
                tokenizedText[i] = ""; // It is always followed by an @ mention
                if (i + 1 < tokenizedText.length - 1) {
                    tokenizedText[i + 1] = "";
                    i++;
                }
            } else {
                break;
            }
        }
    }

    private void cleanTail() { // Removing from the end
        for (int i = tokenizedText.length - 1; i > 0; i--) {
            if (tokenizedText[i].startsWith("#")) {
                //  tokenizedText[i] = "";
            } else if (tokenizedText[i].startsWith("@")) {
                //Don't touch
            } else if (tokenizedText[i].matches("(http)s?://.*")) {
                tokenizedText[i] = "";
            } else {
                break;
            }
        }
    }

    /**
     * Rebuilds the tokenized text, separating it at the end of a sentence.
     */
    private void rebuildIt() {
        String cleanSentence = "";
        for (String s : tokenizedText) {
            if (!s.equals("")) {
                cleanSentence += s.trim() + " ";
                if (s.endsWith(".") || s.endsWith("!") || s.endsWith("?")) {
                    ready.add(cleanSentence.trim());
                    cleanSentence = "";
                }
            }
        }
        ready.add(cleanSentence.trim()); // Adding last sentence

        for (int i = 0; i < ready.size(); i++) {//removes empty or small sentences
            if (ready.get(i).matches("\\s+") || ready.get(i).length() < 2) {
                ready.remove(i);
            }
        }
    }
}

