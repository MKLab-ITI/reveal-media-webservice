package gr.iti.mklab.reveal.util;

import com.google.gson.Gson;

import java.io.*;

/**
 * Created by kandreadou on 10/2/14.
 */
public class NamedEntityImporter {

    private final Gson gson = new Gson();


    public static void main(String[] args) throws Exception {
        NamedEntityImporter nei = new NamedEntityImporter();
        nei.parse();
    }

    public void parse() throws Exception {
        String path = "/home/kandreadou/Pictures/snow_named_entities.json";
        NamedEntityDAO dao = new NamedEntityDAO("160.40.51.20", "Showcase", "NamedEntities");

        BufferedReader br = new BufferedReader(new FileReader(new File(path)));
        String line = "";
        while ((line = br.readLine()) != null) {
            EntityForTweet tweet = gson.fromJson(line, EntityForTweet.class);
            dao.addItem(tweet);
            //System.out.println(tweet);
        }

    }

}
