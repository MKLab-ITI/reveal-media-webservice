package gr.iti.mklab.reveal.web;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;


/**
 * Created by kandreadou on 11/14/14.
 */
public class StatisticsResult implements Serializable {

    @Expose
    @SerializedName("name")
    private String collectionName;

    @Expose
    @SerializedName("size")
    private int collectionSize;

    public StatisticsResult(String name, int size) {
        this.collectionName = name;
        this.collectionSize = size;
    }
}
