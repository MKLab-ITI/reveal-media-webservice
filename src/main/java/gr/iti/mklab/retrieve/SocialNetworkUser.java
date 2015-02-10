package gr.iti.mklab.retrieve;

import com.google.api.services.youtube.model.Channel;
import com.google.api.services.youtube.model.ChannelStatistics;
import com.google.gdata.data.Link;
import com.google.gdata.data.media.mediarss.MediaThumbnail;
import com.google.gdata.data.youtube.UserProfileEntry;
import com.google.gdata.data.youtube.YtUserProfileStatistics;
import gr.iti.mklab.simmo.UserAccount;
import org.mongodb.morphia.annotations.Entity;

/**
 * Created by kandreadou on 1/26/15.
 */
@Entity("UserAccount")
public class SocialNetworkUser extends UserAccount {

    protected String pageUrl;

    protected String location;

    protected String description;

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

    public SocialNetworkUser(UserProfileEntry user) {
        if (user == null)
            return;
        setSource("YouTube#" + user.getUsername());
        //setStreamId("YouTube");

        //The name of the user
        name = (user.getFirstName() == null ? "" : user.getFirstName() + " ") + (user.getLastName() == null ? "" : user.getLastName());


        Link link = user.getLink("alternate", "text/html");
        if (link != null) {
            pageUrl = link.getHref();
        }
        location = user.getLocation();
        description = user.getAboutMe();
        MediaThumbnail thumnail = user.getThumbnail();
        setAvatarBig(thumnail.getUrl());
        YtUserProfileStatistics statistics = user.getStatistics();
        if (statistics != null) {
            setNumFollowers((int) statistics.getSubscriberCount());
        }
    }
}
