package gr.iti.mklab.retrieve;

import gr.iti.mklab.simmo.items.Video;
import org.mongodb.morphia.annotations.Entity;

/**
 * Created by kandreadou on 1/22/15.
 */
@Entity("Video")
public class SocialNetworkVideo extends Video {

    protected String socialNetworkId;

    protected String networkName;

    protected long numLikes;

    protected long numViews;

    protected float rating;

    public SocialNetworkVideo() {
    }
}

