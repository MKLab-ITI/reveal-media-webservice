package gr.iti.mklab.reveal.crawler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import gr.iti.mklab.sm.feeds.Feed;
import gr.iti.mklab.sm.feeds.GeoFeed;
import gr.iti.mklab.sm.feeds.KeywordsFeed;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

/**
 * A simple client for handling the simmo stream manager web service
 *
 * @author kandreadou
 */
public class StreamManagerClient {

    private String webServiceHost;

    private HttpClient httpClient;

    public StreamManagerClient(String webServiceHost) {
        this.webServiceHost = webServiceHost;
        MultiThreadedHttpConnectionManager cm = new MultiThreadedHttpConnectionManager();
        HttpConnectionManagerParams params = new HttpConnectionManagerParams();
        params.setMaxTotalConnections(50);
        params.setDefaultMaxConnectionsPerHost(20);
        params.setConnectionTimeout(10000);
        cm.setParams(params);
        this.httpClient = new HttpClient(cm);
    }

    public void addAllKeywordFeeds(Set<String> keywords, String collection) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, -1);
        KeywordsFeed flickr = new KeywordsFeed();
        flickr.addKeywords(new ArrayList(keywords));
        flickr.setId(collection + "Flickr#1");
        flickr.setSinceDate(cal.getTime());
        flickr.setSource("Flickr");
        flickr.setLabel(collection);
        addKeywordsFeed(flickr);

        KeywordsFeed feed = new KeywordsFeed();
        feed.addKeywords(new ArrayList(keywords));
        feed.setId(collection + "Twitter#1");
        feed.setSinceDate(cal.getTime());
        feed.setSource("Twitter");
        feed.setLabel(collection);
        addKeywordsFeed(feed);

        KeywordsFeed instagram = new KeywordsFeed();
        instagram.addKeywords(new ArrayList(keywords));
        instagram.setId(collection + "Instagram#1");
        instagram.setSinceDate(cal.getTime());
        instagram.setSource("Instagram");
        instagram.setLabel(collection);
        addKeywordsFeed(instagram);

        KeywordsFeed tumblr = new KeywordsFeed();
        tumblr.addKeywords(new ArrayList(keywords));
        tumblr.setId(collection + "Tumblr#1");
        tumblr.setSinceDate(cal.getTime());
        tumblr.setSource("Tumblr");
        tumblr.setLabel(collection);
        addKeywordsFeed(tumblr);

        KeywordsFeed youtube = new KeywordsFeed();
        youtube.addKeywords(new ArrayList(keywords));
        youtube.setId(collection + "Youtube#1");
        youtube.setSinceDate(cal.getTime());
        youtube.setSource("YouTube");
        youtube.setLabel(collection);
        addKeywordsFeed(youtube);
    }

    public void addAllGeoFeeds(double lon_min, double lat_min, double lon_max, double lat_max, String collection) {

        double density = Math.abs((lon_max - lon_min) / 100);
        GeoFeed streetview = new GeoFeed(lon_min, lat_min, lon_max, lat_max, density);
        streetview.setId(collection + "StreetView#1");
        streetview.setSource("StreetView");
        streetview.setLabel(collection);
        addGeoFeed(streetview);
        GeoFeed panoramio = new GeoFeed(lon_min, lat_min, lon_max, lat_max);
        panoramio.setId(collection + "Panoramio#1");
        panoramio.setSource("Panoramio");
        panoramio.setLabel(collection);
        addGeoFeed(panoramio);
        GeoFeed wikimapia = new GeoFeed(lon_min, lat_min, lon_max, lat_max);
        wikimapia.setId(collection + "Wikimapia#1");
        wikimapia.setSource("Wikimapia");
        wikimapia.setLabel(collection);
        addGeoFeed(wikimapia);
    }

    public void deleteAllFeeds(boolean isGeo, String collection){
        if(isGeo){
            deleteFeed(collection+"StreetView#1");
            deleteFeed(collection+"Panoramio#1");
            deleteFeed(collection+"Wikimapia#1");
        }else{
            deleteFeed(collection+"Flickr#1");
            deleteFeed(collection+"Twitter#1");
            deleteFeed(collection+"Instagram#1");
            deleteFeed(collection+"Tumblr#1");
            deleteFeed(collection+"Youtube#1");
        }
    }

    public String addKeywordsFeed(KeywordsFeed kfeed) {
        return addFeed("/sm/sm/feeds/addkeywords", kfeed);
    }

    public String addGeoFeed(GeoFeed gfeed) {
        return addFeed("/sm/sm/feeds/addgeo", gfeed);
    }

    private String addFeed(String path, Feed feed) {
        Gson gson = new GsonBuilder().create();
        PostMethod queryMethod = null;
        String response = null;
        try {
            queryMethod = new PostMethod(webServiceHost + path);
            queryMethod.setRequestEntity(new StringRequestEntity(gson.toJson(feed), "application/json", "UTF-8"));
            int code = httpClient.executeMethod(queryMethod);
            if (code == 200) {
                response = queryMethod.getResponseBodyAsString();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (queryMethod != null) {
                queryMethod.releaseConnection();
            }
        }
        return response;
    }

    private String deleteFeed(String id) {
        GetMethod queryMethod = null;
        String response = null;
        try {
            queryMethod = new GetMethod(webServiceHost + "/sm/sm/feeds/delete");
            queryMethod.setQueryString("id=" + id);
            int code = httpClient.executeMethod(queryMethod);
            if (code == 200) {
                response = queryMethod.getResponseBodyAsString();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (queryMethod != null) {
                queryMethod.releaseConnection();
            }
        }
        return response;
    }

    public static void main(String[] args) {
        Set<String> keywords = new HashSet<>();
        keywords.add("grexit");
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, -1);
        KeywordsFeed flickr = new KeywordsFeed();
        flickr.addKeywords(new ArrayList(keywords));
        flickr.setId("Flickr#2");
        flickr.setSinceDate(cal.getTime());
        flickr.setSource("Flickr");
        flickr.setLabel("tFlickr1");

        StreamManagerClient client = new StreamManagerClient("http://127.0.0.1:8080");
        client.deleteFeed(flickr.getId());
    }

}
