package gr.iti.mklab.reveal.util;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import eu.socialsensor.framework.client.dao.StreamUserDAO;
import eu.socialsensor.framework.client.dao.impl.StreamUserDAOImpl;
import eu.socialsensor.framework.common.domain.StreamUser;
import gr.iti.mklab.reveal.mongo.RevealMediaClusterDaoImpl;
import gr.iti.mklab.reveal.mongo.RevealMediaItemDaoImpl;
import gr.iti.mklab.reveal.visual.IndexingManager;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.util.DateUtil;
import org.bson.BSONObject;
import org.bson.types.ObjectId;

import java.io.*;
import java.util.*;

/**
 * Created by kandreadou on 10/6/14.
 */
public class TextImporter {

    public static void main(String[] args) throws Exception {
        TextImporter ti = new TextImporter();
        ti.importClustersFromDBSCAN();
    }

    private void importClustersFromDBSCAN() throws Exception {
        RevealMediaClusterDaoImpl clusterDao = new RevealMediaClusterDaoImpl("160.40.51.20", "Showcase", "MediaClustersDBSCAN1");
        String rootFolder = "/home/kandreadou/Pictures/clustertest_10000/dbscan";

        for (File f : new File(rootFolder).listFiles()) {
            List<String> myList = new ArrayList<>();
            if (f.isDirectory() && f.listFiles().length < 1500) {
                MediaCluster c = new MediaCluster(new ObjectId().toString());
                for (File image : f.listFiles()) {
                    String id = image.getName().substring(0, image.getName().lastIndexOf('.'));
                    myList.add(id);
                    //c.addMember(id);
                }
                List<String> newList = Lists.reverse(myList);
                for (String s : newList)
                    c.addMember(s);
                c.setCount(c.getMembers().size());
                clusterDao.saveCluster(c);
            }
        }
        /*List<MediaCluster> clusters = clusterDao.getSortedClusters(0, 7278);
        for (MediaCluster c : clusters) {
            clusterDao.updateCluster(c.getId(), c.getMembers().size());
        }*/
    }


    private void deleteBlankImages() throws Exception {
        IndexingManager.getInstance();
        RevealMediaItemDaoImpl mediaDao = new RevealMediaItemDaoImpl("160.40.51.20", "Showcase", "MediaItems");
        List<MediaItem> items = mediaDao.getMediaItems(10000, 34110, "image");
        for (MediaItem item : items) {
            try {
                boolean isBlank = IndexingManager.getInstance().isImageBlank(item.getUrl());
                if (isBlank)
                    System.out.println("Image " + item.getUrl() + "and id " + item.getId() + " is blank");
            } catch (Exception ex) {
                System.out.println("Image " + item.getUrl() + "and id " + item.getId() + " is error");
            }
        }
    }

    private void sanitizeImageSizes() throws Exception {
        RevealMediaItemDaoImpl mediaDao = new RevealMediaItemDaoImpl("160.40.51.20", "Showcase", "MediaItems");
        int nullCounter = 0;
        int smallCounter = 0;
        List<MediaItem> items = mediaDao.getMediaItems(0, 35105, "image");
        for (MediaItem item : items) {
            if (item.getWidth() == null || item.getHeight() == null)
                mediaDao.removeMediaItem(item.getId());
            else if (item.getHeight() < 200 && item.getWidth() < 200)
                mediaDao.removeMediaItem(item.getId());
        }
    }

    // This is needed because of the way the storm focused crawler stores the video
    // stream users (e.g. Youtube).
    private void sanitizeStreamUsers() throws Exception {
        StreamUserDAOImpl userDAO = new StreamUserDAOImpl("160.40.51.20", "Showcase", "StreamUsers");
        StreamUserDAO.StreamUserIterator it = userDAO.getIterator(new BasicDBObject("profileImage", new BasicDBObject("$exists", true)));
        while (it.hasNext()) {
            StreamUser s = it.next();
            s.setUrl(s.getPageUrl());
            s.setImageUrl(s.getProfileImage());
            userDAO.updateStreamUser(s);
        }
    }

    // Delete non existing cluster members
    private void sanitizeMediaClusters() throws Exception {
        RevealMediaClusterDaoImpl clusterDao = new RevealMediaClusterDaoImpl("160.40.51.20", "Showcase", "MediaClusters");
        List<MediaCluster> clusters = clusterDao.getSortedClusters(0, 7278);
        for (MediaCluster c : clusters) {
            clusterDao.updateCluster(c.getId(), c.getMembers().size());
        }
    }

    private void importUsersFromFiles() throws Exception {
        int count = 0;
        RevealMediaItemDaoImpl mediaDao = new RevealMediaItemDaoImpl("160.40.51.20", "Showcase", "MediaItems");
        StreamUserDAOImpl userDAO = new StreamUserDAOImpl("160.40.51.20", "Showcase", "StreamUsers");
        BufferedReader reader;
        String jsonFilesFolder = "/home/kandreadou/Pictures/snow/";
        JsonParser parser = new JsonParser();
        List<String> jsonFiles = new ArrayList<String>();
        for (int i = 0; i < 42; i++) {
            jsonFiles.add(jsonFilesFolder + "tweets.json." + i);
        }

        for (int i = 30; i < jsonFiles.size(); i++) {

            System.out.println(jsonFiles.get(i));
            reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(jsonFiles.get(i)), "UTF-8"));
            String line = null;
            while ((line = reader.readLine()) != null) {
                count++;
                if (count == 930) {
                    System.out.println(line);
                }
                try {
                    JsonObject tweet = parser.parse(line).getAsJsonObject();
                    String tweetId = tweet.get("id").getAsString();
                    JsonObject user = tweet.get("user").getAsJsonObject();
                    if (user == null) {
                        System.out.println("USER IS NULL");
                    }
                    if (user != null) {
                        String userId = user.get("id").getAsString();
                        StreamUser su = new StreamUser(tweetId, StreamUser.Operation.UPDATE);
                        if (!user.get("description").isJsonNull())
                            su.setDescription(user.get("description").getAsString());
                        su.setId(userId);
                        if (!user.get("url").isJsonNull())
                            su.setPageUrl(user.get("url").getAsString());
                        if (!user.get("name").isJsonNull())
                            su.setName(user.get("name").getAsString());
                        if (!user.get("profile_image_url").isJsonNull())
                            su.setImageUrl(user.get("profile_image_url").getAsString());
                        if (user.has("followers_count"))
                            su.setFollowers(user.get("followers_count").getAsLong());
                        if (!user.get("screen_name").isJsonNull()) {
                            String screenName = user.get("screen_name").getAsString();
                            su.setUserid(screenName);
                            su.setUrl("https://twitter.com/" + screenName);
                        }

                        MediaItem item = (MediaItem) mediaDao.getMediaItem(tweetId);

                        if (item != null) {
                            //System.out.println(item);
                            item.setUserId(userId);
                            //System.out.println(user);
                            //mediaDao.updateMediaItem(item);
                            if (!userDAO.exists(su.getId()))
                                userDAO.insertStreamUser(su);
                        }
                    }
                } catch (Exception ex) {
                    System.out.println(ex + "line " + line);
                }
            }
            reader.close();
        }
    }

    private void importFromFiles() throws Exception {
        RevealMediaItemDaoImpl mediaDao = new RevealMediaItemDaoImpl("160.40.51.20", "Showcase", "MediaItems");
        BufferedReader reader;
        String jsonFilesFolder = "/home/kandreadou/Pictures/snow/";
        JsonParser parser = new JsonParser();
        List<String> jsonFiles = new ArrayList<String>();
        for (int i = 0; i < 42; i++) {
            jsonFiles.add(jsonFilesFolder + "tweets.json." + i);
        }

        for (int i = 30; i < jsonFiles.size(); i++) {
            System.out.println(jsonFiles.get(i));
            reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(jsonFiles.get(i)), "UTF-8"));
            String line = null;
            while ((line = reader.readLine()) != null) {
                JsonObject tweet = parser.parse(line).getAsJsonObject();
                String tweetId = tweet.get("id").getAsString();
                MediaItem item = (MediaItem) mediaDao.getMediaItem(tweetId);
                if (item != null) {
                    System.out.println(item);
                    if (StringUtils.isEmpty(item.getDescription())) {
                        String text = tweet.get("text").getAsString();
                        System.out.println(text);
                        item.setDescription(text);
                    }
                    if (item.getPublicationTime() == 0) {
                        String created_at = tweet.get("created_at").getAsString();
                        System.out.println(created_at);
                        item.setPublicationTime(DateUtil.parseDate(created_at).getTime());
                    }
                    mediaDao.updateMediaItem(item);
                }
            }
            reader.close();
        }
    }
}
