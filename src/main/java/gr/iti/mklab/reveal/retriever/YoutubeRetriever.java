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
import java.util.List;

/**
 * Created by kandreadou on 1/15/15.
 */
public class YoutubeRetriever {

    private final String activityFeedUserUrlPrefix = "http://gdata.youtube.com/feeds/api/users/";
    private final String activityFeedVideoUrlPrefix = "http://gdata.youtube.com/feeds/api/videos";
    private final String uploadsActivityFeedUrlSuffix = "/uploads";
    private Logger logger = Logger.getLogger(YoutubeRetriever.class);

    private YouTubeService service;
    private int results_threshold;
    private int request_threshold;
    private long maxRunningTime;

    private final static String APP_NAME = "reveal-2015";
    private final static String DEV_ID = "AIzaSyA_DFJJ63kioLqZ09fH2kvIlqeNMrPvATU";

    public static void main(String[] args) throws Exception{
        YoutubeRetriever r = new YoutubeRetriever(APP_NAME, DEV_ID,100,20,50000);
        List<Video> results = r.retrieveKeywordsFeeds(new String[]{"Ukraine"});
        MorphiaManager.setup("youtube");
        MediaDAO<Video> dao = new MediaDAO<>(Video.class);
        for (Video v:results)
            dao.save(v);
        MorphiaManager.tearDown();
    }
    public YoutubeRetriever(String clientId, String developerKey) {
        this.service = new YouTubeService(clientId, developerKey);
    }

    public YoutubeRetriever(String clientId, String developerKey, int maxResults, int maxRequests, long maxRunningTime) {
        this(clientId, developerKey);
        this.results_threshold = maxResults;
        this.request_threshold = maxRequests;
        this.maxRunningTime = maxRunningTime;
    }

    public List<Video> retrieveKeywordsFeeds(String[] keywords) {

        List<Video> items = new ArrayList<>();

        int startIndex = 1;
        int maxResults = 25;
        int currResults = 0;
        int numberOfRequests = 0;

        long currRunningTime = System.currentTimeMillis();

        boolean isFinished = false;


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

                    if (items.size() > results_threshold || numberOfRequests >= request_threshold || (System.currentTimeMillis() - currRunningTime) > maxRunningTime) {
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
