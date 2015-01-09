package gr.iti.mklab.reveal.web;

import eu.socialsensor.framework.common.domain.MediaItem;
import gr.iti.mklab.reveal.util.EntityForTweet;

import java.io.Serializable;
import java.net.MalformedURLException;

/**
 * THIS IS OBSOLETE AND WILL BE DELETED WHEN NO LONGER NEEDED
 * DO NOT USE THIS CLASS
 * @deprecated
 * @author kandreadou
 */
public class EntityResult implements Serializable {

    @com.google.gson.annotations.Expose
    @com.google.gson.annotations.SerializedName("entities")
    private EntityForTweet.NamedEntity[] entities;

    @com.google.gson.annotations.Expose
    @com.google.gson.annotations.SerializedName("item")
    private MediaItem item;

    public EntityResult(MediaItem item, EntityForTweet.NamedEntity[] entities) throws MalformedURLException {
        this.item = item;
        this.entities = entities;
    }
}
