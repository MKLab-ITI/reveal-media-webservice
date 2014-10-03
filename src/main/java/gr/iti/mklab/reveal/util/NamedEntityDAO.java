package gr.iti.mklab.reveal.util;

import com.google.gson.Gson;
import eu.socialsensor.framework.client.mongo.MongoHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kandreadou on 10/3/14.
 */
public class NamedEntityDAO {

    private Gson gson = new Gson();
    List<String> indexes = new ArrayList<String>();
    private MongoHandler mongoHandler;

    public NamedEntityDAO(String host, String db, String collection) throws Exception {
        indexes.add("id");
        indexes.add("token");
        mongoHandler = new MongoHandler(host, db, collection, indexes);
    }

    public void addItem(EntityForTweet item) {
        mongoHandler.insert(item);
    }

    public EntityForTweet getItemForTweetId(String tweetId){
        String json = mongoHandler.findOne("tweetId", tweetId);
        return gson.fromJson(json, EntityForTweet.class);
    }
}
