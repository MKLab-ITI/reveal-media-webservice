package gr.iti.mklab.retrieve;

import com.google.api.services.youtube.model.Channel;
import com.google.api.services.youtube.model.ChannelStatistics;
import gr.iti.mklab.simmo.core.UserAccount;
import org.mongodb.morphia.annotations.Entity;

/**
 * Created by kandreadou on 1/26/15.
 */
@Entity("UserAccount")
public class SocialNetworkUser extends UserAccount {

    public SocialNetworkUser() {
    }

    public SocialNetworkUser(Channel c){
        setSource("YouTube#" + c.getId());
        //setStreamId("YouTube");
        name = c.getSnippet().getTitle();
        description = c.getSnippet().getDescription();
        com.google.api.services.youtube.model.Thumbnail t = c.getSnippet().getThumbnails().getDefault();
        setAvatarBig(t.getUrl());
        ChannelStatistics s = c.getStatistics();
        if(s!=null){
            setNumFollowers(s.getSubscriberCount().intValue());
        }
        pageUrl = "https://www.youtube.com/channel/"+c.getId();
    }
}
