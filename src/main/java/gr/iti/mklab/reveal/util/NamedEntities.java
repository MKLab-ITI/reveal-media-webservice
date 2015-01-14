package gr.iti.mklab.reveal.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import eu.socialsensor.framework.common.domain.JSONable;
import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

/**
 * Created by kandreadou on 10/3/14.
 */
@Entity
public class NamedEntities implements JSONable {

    @Id
    public ObjectId _id;

    @Expose
    @SerializedName(value = "tweetId")
    public String tweetId;

    @Expose
    @SerializedName(value = "namedEntities")
    public NamedEntity[] namedEntities;


    @Override
    public String toJSONString() {
        Gson gson = new GsonBuilder()
                .excludeFieldsWithoutExposeAnnotation()
                .create();
        return gson.toJson(this);
    }

}
