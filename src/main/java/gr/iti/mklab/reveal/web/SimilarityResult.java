package gr.iti.mklab.reveal.web;

import eu.socialsensor.framework.common.domain.MediaItem;

import java.io.Serializable;
import java.net.MalformedURLException;


/**
 * Created by kandreadou on 9/24/14.
 */
public class SimilarityResult implements Serializable {

    @com.google.gson.annotations.Expose
    @com.google.gson.annotations.SerializedName("distance")
    private double distance;

    @com.google.gson.annotations.Expose
    @com.google.gson.annotations.SerializedName("item")
    private MediaItem item;

    public SimilarityResult(MediaItem item, double distance) throws MalformedURLException {
        this.item = item;
        this.distance = distance;
    }

    /*@Override
    public String toJSONString() {
        Gson gson = new GsonBuilder()
                .excludeFieldsWithoutExposeAnnotation()
                .create();
        return gson.toJson(this);
    }*/

    @Override
    public String toString() {
        return item.toString()+"score "+distance;
    }
}
