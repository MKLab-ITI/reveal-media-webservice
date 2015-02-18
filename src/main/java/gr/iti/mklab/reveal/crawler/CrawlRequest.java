package gr.iti.mklab.reveal.crawler;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * A CrawlRequest class for storing the queue state to MongoDB
 *
 * @author kandreadou
 */
@Entity(noClassnameStored=true)
public class CrawlRequest {

    public enum STATE {
        WAITING, STOPPING, PAUSED, RUNNING, FINISHED, DELETING, STARTING
    }

    /**
     * The current state of the request
     */
    public STATE requestState;

    /**
     * The request creation date
     */
    public Date creationDate;

    /**
     * The date of the latest change of the request state
     * Useful for finding out how long a request has been waiting or running for instance
     */
    public Date lastStateChange;

    /**
     * The path to the crawl data directory (not the warc files rather than
     * the internal crawler files, sieve, frontier etc
     */
    public String crawlDataPath;

    /**
     * The collection name, for naming the index and the mongo db table
     */
    public String collectionName;

    /**
     * The number of crawled images
     */
    //public int numImages;

    /**
     * A list of keywords to focus the crawl on
     */
    public Set<String> keywords = new HashSet<String>();
    /**
     * Will be this a new or an already existing crawl?
     * By default start a new crawl
     */
    public boolean isNew = true;

    /** The request's unique id */
    @Id
    public String id = new ObjectId().toString();

}

