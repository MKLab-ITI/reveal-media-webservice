package gr.iti.mklab.reveal.web;

import java.util.HashSet;
import java.util.Set;

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
