package gr.iti.mklab.reveal.crawler;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

import java.util.Date;

/**
 * A CrawlRequest class for storing the queue state to MongoDB
 *
 * @author kandreadou
 */
@Entity
public class CrawlRequest {

    public enum STATE {
        WAITING, PENDING, PAUSED, RUNNING, FINISHED
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
     * The JMX port number, necessary for invoking the JMX methods
     * and differentiating among the different BUbiNG agents
     */
    public int portNumber;

    /**
     * The path to the crawl data directory (not the warc files rather than
     * the internal crawler files, sieve, frontier etc
     */
    public String crawlDataPath;

    /**
     * The collection name, for naming the index and the mongo db table
     */
    public String collectionName;

    /** The request's unique id */
    @Id
    public ObjectId id;

}

