package gr.iti.mklab.reveal.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import eu.socialsensor.framework.common.domain.JSONable;

/**
 * Created by kandreadou on 10/3/14.
 */
public class EntityForTweet implements JSONable{

    @Expose
    @SerializedName(value = "tweetId")
    public String tweetId;

    @Expose
    @SerializedName(value = "namedEntities")
    public NamedEntity[] namedEntities;

    public class NamedEntity {
        @Expose
        @SerializedName(value = "token")
        public String token;

        @Expose
        @SerializedName(value = "type")
        public String type;
    }

    @Override
    public String toJSONString() {
        Gson gson = new GsonBuilder()
                .excludeFieldsWithoutExposeAnnotation()
                .create();
        return gson.toJson(this);
    }
}
