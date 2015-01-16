package gr.iti.mklab.reveal.retriever;

import com.google.gdata.client.youtube.YouTubeQuery;
import com.google.gdata.client.youtube.YouTubeService;
import com.google.gdata.data.youtube.VideoEntry;
import com.google.gdata.data.youtube.VideoFeed;
import gr.iti.mklab.simmo.items.Video;
import gr.iti.mklab.simmo.morphia.MediaDAO;
import gr.iti.mklab.simmo.morphia.MorphiaManager;
import org.apache.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A YoutubeRetriever that retrieves videos from youtube given a set of keywords
 * Based on the socialsensor source code, adapted for the needs of this project
 *
 * @author kandreadou
 */
public class YoutubeRetriever {

    private final String activityFeedVideoUrlPrefix = "http://gdata.youtube.com/feeds/api/videos";
    private Logger logger = Logger.getLogger(YoutubeRetriever.class);

    private YouTubeService service;
    // This is an API restriction
    private final static int RESULTS_THRESHOLD = 500;
    private final static int REQUEST_THRESHOLD = 50;
    private final static long MAX_RUNNING_TIME = 120000; //2 MINUTES

    private final static String APP_NAME = "reveal-2015";
    private final static String DEV_ID = "AIzaSyA_DFJJ63kioLqZ09fH2kvIlqeNMrPvATU";

    public static void main(String[] args) throws Exception {
        YoutubeRetriever r = new YoutubeRetriever();
        Set<String> set = new HashSet<>();
        set.add("Ukraine");
        set.add("merkel");
        set.add("intervention");
        List<Video> results = r.retrieveKeywordsFeeds(set);
        MorphiaManager.setup("youtube");
        MediaDAO<Video> dao = new MediaDAO<>(Video.class);
        for (Video v : results)
            dao.save(v);
        //SocialNetworkVideo v = (SocialNetworkVideo) dao.get(new ObjectId("54b7dc7c00b0d4e0cd2f784b"));
        MorphiaManager.tearDown();
    }

    public YoutubeRetriever() {
        this.service = new YouTubeService(APP_NAME, DEV_ID);
    }

    public List<Video> retrieveKeywordsFeeds(Set<String> keywords) {

        List<Video> items = new ArrayList<>();

        int startIndex = 1;
        int maxResults = 25;
        int currResults = 0;
        int numberOfRequests = 0;

        long currRunningTime = System.currentTimeMillis();

        if (keywords == null) {
            logger.error("#YouTube : No keywords feed");
            return items;
        }

        String tags = "";

        for (String key : keywords) {
            String[] words = key.split(" ");
            for (String word : words) {
                if (!tags.contains(word) && word.length() > 1)
                    tags += word.toLowerCase() + " ";
            }
        }

        //one call - 25 results
        if (tags.equals(""))
            return items;

        YouTubeQuery query;
        try {
            query = new YouTubeQuery(new URL(activityFeedVideoUrlPrefix));
        } catch (MalformedURLException e1) {

            return items;
        }

        query.setOrderBy(YouTubeQuery.OrderBy.PUBLISHED);
        query.setFullTextQuery(tags);
        query.setSafeSearch(YouTubeQuery.SafeSearch.NONE);
        query.setMaxResults(maxResults);

        VideoFeed videoFeed;
        while (true) {
            try {
                query.setStartIndex(startIndex);
                videoFeed = service.query(query, VideoFeed.class);

                numberOfRequests++;

                currResults = videoFeed.getEntries().size();
                startIndex += currResults;

                for (VideoEntry video : videoFeed.getEntries()) {
                    Video videoItem = new SocialNetworkVideo(video);
                    items.add(videoItem);

                    if (items.size() > RESULTS_THRESHOLD || numberOfRequests >= REQUEST_THRESHOLD || (System.currentTimeMillis() - currRunningTime) > MAX_RUNNING_TIME) {
                        return items;
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
                logger.error("YouTube Retriever error during retrieval of " + tags);
                logger.error("Exception: " + e.getMessage());
                return items;
            }
        }

    }
}
