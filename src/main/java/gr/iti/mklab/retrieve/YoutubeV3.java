package gr.iti.mklab.retrieve;

import gr.iti.mklab.simmo.morphia.MorphiaManager;
import org.apache.commons.lang.StringUtils;
import org.bson.types.ObjectId;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Joiner;
import com.google.api.services.youtube.YouTube;
import com.google.api.services.youtube.model.*;
import com.google.api.services.youtube.model.Video;

import java.io.IOException;
import java.util.*;

/**
 * Created by kandreadou on 2/10/15.
 */
public class YoutubeV3 {
    /**
     * Define a global instance of a Youtube object, which will be used
     * to make YouTube Data API requests.
     */
    private static YouTube youtube;

    /**
     * API restriction: Values must be within the range: [0, 50]
     */
    private static final long NUMBER_OF_VIDEOS_RETURNED = 50;

    private final static String apiKey = "AIzaSyA_DFJJ63kioLqZ09fH2kvIlqeNMrPvATU";

    private boolean stop = false;

    private Thread thread;

    //private MediaDAO<SocialNetworkVideo> videoDAO;

    public static void main(String[] args) throws Exception {
        MorphiaManager.setup("127.0.0.1");

        Set<String> keywords = new HashSet<String>();
        keywords.add("tsipras");
        keywords.add("syriza");
        keywords.add("election");
    }

    public YoutubeV3() throws Exception {

        youtube = new YouTube.Builder(new NetHttpTransport(), new JacksonFactory(), new HttpRequestInitializer() {
            public void initialize(HttpRequest request) throws IOException {
            }
        }).setApplicationName("reveal-2015").build();

        //videoDAO = new MediaDAO<SocialNetworkVideo>(SocialNetworkVideo.class, collectionName);
    }

    public void stop() {

        stop = true;
        if(thread!=null && thread.isAlive())
            thread.interrupt();
    }

    public void collect(final Set<String> keywords) {

        thread = new Thread(new Runnable() {
            @Override
            public void run() {
                get(keywords);
                MorphiaManager.tearDown();
            }
        });
        thread.start();
    }

    private void get(Set<String> keywords) {

        String queryTerm = "";

        for (String key : keywords) {
            String[] words = key.split(" ");
            for (String word : words) {
                if (!queryTerm.contains(word) && word.length() > 1)
                    queryTerm += word.toLowerCase() + " ";
            }
        }
        int count = 100;
        String pageToken = null;
        //while (VisualIndexer.videoDAO.count() < count && !stop) {
        while(true){
            try {
                System.out.println("### NEW PAGE");
                // Define the API request for retrieving search results.
                YouTube.Search.List search = youtube.search().list("id,snippet");
                search.setKey(apiKey);
                search.setQ(queryTerm);
                search.setPageToken(pageToken);

                // Restrict the search results to only include videos. See:
                // https://developers.google.com/youtube/v3/docs/search/list#type
                search.setType("video");

                // To increase efficiency, only retrieve the fields that the
                // application uses.
                //search.setFields("pageInfo,items(id/kind,id/videoId,snippet/channelId,snippet/title,snippet/description,snippet/publishedAt,snippet/thumbnails/default/url)");
                search.setMaxResults(NUMBER_OF_VIDEOS_RETURNED);
                //search.setPageToken();

                // Call the API and print results.
                SearchListResponse searchResponse = search.execute();
                List<SearchResult> searchResultList = searchResponse.getItems();
                List<String> videoIds = new ArrayList<String>();
                List<String> channelIds = new ArrayList<String>();

                if (searchResultList != null) {

                    // Merge video IDs
                    for (SearchResult searchResult : searchResultList) {
                        videoIds.add(searchResult.getId().getVideoId());
                        String cid = searchResult.getSnippet().getChannelId();
                        if (StringUtils.isNotEmpty(cid))
                            channelIds.add(cid);
                    }
                    Joiner stringJoiner = Joiner.on(',');
                    String videoId = stringJoiner.join(videoIds);
                    Joiner channelJoiner = Joiner.on(',');
                    String channelId = channelJoiner.join(channelIds);

                    // Call the YouTube Data API's youtube.videos.list method to
                    // retrieve the resources that represent the specified videos.
                    YouTube.Videos.List listVideosRequest = youtube.videos().list("snippet, statistics, contentDetails, recordingDetails").setId(videoId);
                    listVideosRequest.setKey(apiKey);
                    VideoListResponse listResponse = listVideosRequest.execute();

                    List<Video> videoList = listResponse.getItems();

                    YouTube.Channels.List listChannelsRequest = youtube.channels().list("snippet, statistics, contentDetails").setId(channelId);
                    listChannelsRequest.setKey(apiKey);
                    ChannelListResponse channelResponse = listChannelsRequest.execute();
                    List<Channel> channelList = channelResponse.getItems();

                    for (int i = 0; i < videoList.size(); i++) {
                        SocialNetworkVideo newV = new SocialNetworkVideo(videoList.get(i), channelList.get(i));
                        newV.setId(new ObjectId().toString());
                        //VisualIndexer.indexAndStore(newV);
                    }
                    /*if (videoList != null) {
                        prettyPrint(videoList.iterator(), queryTerm);
                    }*/
                }
                pageToken = searchResponse.getNextPageToken();
                count = searchResponse.getPageInfo().getTotalResults() - 10;
            } catch (GoogleJsonResponseException e) {
                System.err.println("There was a service error: " + e.getDetails().getCode() + " : "
                        + e.getDetails().getMessage() + " " + e.getMessage());
            } catch (IOException e) {
                System.err.println("There was an IO error: " + e.getCause() + " : " + e.getMessage());
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

    }

    /*
     * Prints out all results in the Iterator. For each result, print the
     * title, video ID, location, and thumbnail.
     *
     * @param iteratorVideoResults Iterator of Videos to print
     *
     * @param query Search query (String)
     */
    private static void prettyPrint(Iterator<Video> iteratorVideoResults, String query) {

        System.out.println("\n=============================================================");
        System.out.println(
                "   First " + NUMBER_OF_VIDEOS_RETURNED + " videos for search on \"" + query + "\".");
        System.out.println("=============================================================\n");

        if (!iteratorVideoResults.hasNext()) {
            System.out.println(" There aren't any results for your query.");
        }

        while (iteratorVideoResults.hasNext()) {

            Video singleVideo = iteratorVideoResults.next();

            Thumbnail thumbnail = singleVideo.getSnippet().getThumbnails().getDefault();
            //GeoPoint location = singleVideo.getRecordingDetails().getLocation();
            String url = "https://www.youtube.com/watch?v=" + singleVideo.getId();
            System.out.println(" Video Id: " + singleVideo.getId());
            System.out.println(" Title: " + singleVideo.getSnippet().getTitle());
            System.out.println(" Description: " + singleVideo.getSnippet().getDescription());
            System.out.println(" Published at: " + singleVideo.getSnippet().getPublishedAt());
            System.out.println(" Thumbnail: " + thumbnail.getUrl());
            System.out.println(" Comment count: " + singleVideo.getStatistics().getCommentCount());
            System.out.println(" Dimension: " + singleVideo.getContentDetails().getDimension());
            System.out.println(" Duration: " + singleVideo.getContentDetails().getDuration());
            System.out.println(" Definition: " + singleVideo.getContentDetails().getDefinition());
            System.out.println(" ChannelId: " + singleVideo.getSnippet().getChannelId());
            System.out.println(" Channel title: " + singleVideo.getSnippet().getChannelTitle());
            if (singleVideo.getRecordingDetails() != null)
                System.out.println(" Location: " + singleVideo.getRecordingDetails().getLocationDescription());
            System.out.println("\n-------------------------------------------------------------\n");
        }
    }

}
