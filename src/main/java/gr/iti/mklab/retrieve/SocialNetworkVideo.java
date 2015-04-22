package gr.iti.mklab.retrieve;

import com.google.api.services.youtube.model.Channel;
import com.google.api.services.youtube.model.VideoContentDetails;
import com.google.api.services.youtube.model.VideoStatistics;
import gr.iti.mklab.simmo.core.items.Video;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;

import java.util.Date;
import java.util.List;

/**
 * Created by kandreadou on 1/22/15.
 */
@Entity("Video")
public class SocialNetworkVideo extends Video {

    @Embedded
    public SocialNetworkUser user;

    public SocialNetworkVideo(){}

    public SocialNetworkVideo(com.google.api.services.youtube.model.Video v, Channel c) {
        id = "Youtube#"+v.getId();
        source = "Youtube";
        title = v.getSnippet().getTitle();
        description = v.getSnippet().getDescription();
        creationDate = new Date(v.getSnippet().getPublishedAt().getValue());
        crawlDate = new Date();
        VideoStatistics statistics = v.getStatistics();
        if(statistics!=null){
            numLikes = statistics.getFavoriteCount().intValue();
            numViews = statistics.getViewCount().intValue();
        }
        VideoContentDetails details = v.getContentDetails();
        if(details!=null){
            quality = details.getDefinition();
        }
        com.google.api.services.youtube.model.Thumbnail t = v.getSnippet().getThumbnails().getHigh();
        setThumbnail(t.getUrl());
        setWidth(t.getWidth().intValue());
        setHeight(t.getHeight().intValue());
        url =  "https://www.youtube.com/watch?v=" + v.getId();
        webPageUrl = url;

        user = new SocialNetworkUser(c);
    }

}

