package gr.iti.mklab.reveal.web;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import eu.socialsensor.framework.common.domain.JSONable;
import eu.socialsensor.framework.common.domain.MediaItem;

import java.io.Serializable;
import java.net.MalformedURLException;


/**
 * Created by kandreadou on 9/24/14.
 */
public class SimilarityResult implements JSONable, Serializable {

    @com.google.gson.annotations.Expose
    @com.google.gson.annotations.SerializedName("score")
    private double score;

    @com.google.gson.annotations.Expose
    @com.google.gson.annotations.SerializedName("item")
    private MediaItem item;

    public SimilarityResult(MediaItem item, double score) throws MalformedURLException {
        this.item = item;
        this.score = score;
    }

    @Override
    public String toJSONString() {
        Gson gson = new GsonBuilder()
                .excludeFieldsWithoutExposeAnnotation()
                .create();
        return gson.toJson(this);
    }

    @Override
    public String toString() {
        return item.toString()+"score "+score;
    }
}
