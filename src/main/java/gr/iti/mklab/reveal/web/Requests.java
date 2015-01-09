package gr.iti.mklab.reveal.web;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by kandreadou on 1/9/15.
 */
public final class Requests {

    /**
     * Created by kandreadou on 1/8/15.
     * {
     * "collectionName":"col",
     * "isNew":true,
     * "keywords":[
     * "malaysia",
     * "disaster",
     * "missing",
     * "airplane",
     * "boeing",
     * "injured"
     * ]
     * }
     */
    public class CrawlPostRequest {

        public Set<String> keywords = new HashSet<>();

        public String collectionName;

        public boolean isNew;
    }

    /**
     * {"collection":"today","urls":["http://static4.businessinsider.com/image/5326130f69bedd780c549606-1200-924/putin-68.jpg","http://www.trbimg.com/img-531a4ce6/turbine/topic-peplt007593"]
     * }
     */
    public class IndexPostRequest {

        protected String collection;

        public Set<String> urls = new HashSet<>();
    }
}
