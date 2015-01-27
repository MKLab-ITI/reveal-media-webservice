package gr.iti.mklab.retrieve;

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
}
