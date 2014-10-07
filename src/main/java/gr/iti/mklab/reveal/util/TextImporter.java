package gr.iti.mklab.reveal.util;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import eu.socialsensor.framework.client.dao.impl.StreamUserDAOImpl;
import eu.socialsensor.framework.common.domain.MediaItem;
import eu.socialsensor.framework.common.domain.StreamUser;
import gr.iti.mklab.reveal.mongo.RevealMediaItemDaoImpl;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.util.DateUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by kandreadou on 10/6/14.
 */
public class TextImporter {

    public static void main(String[] args) throws Exception {
        TextImporter ti = new TextImporter();
        ti.importUsersFromFiles();
    }

    private void importUsersFromFiles() throws Exception {
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
                try {
                    JsonObject tweet = parser.parse(line).getAsJsonObject();
                    String tweetId = tweet.get("id").getAsString();
                    JsonObject user = tweet.get("user").getAsJsonObject();
                    if (user != null) {
                        String userId = user.get("id").getAsString();
                        StreamUser su = new StreamUser(tweetId, StreamUser.Operation.UPDATE);
                        if (user.has("description"))
                            su.setDescription(user.get("description").getAsString());
                        su.setId(userId);
                        if (user.has("url"))
                            su.setPageUrl(user.get("url").getAsString());
                        if (user.has("name"))
                            su.setName(user.get("name").getAsString());
                        if (user.has("profile_image_url"))
                            su.setImageUrl(user.get("profile_image_url").getAsString());
                        if (user.has("followers_count"))
                            su.setFollowers(user.get("followers_count").getAsLong());
                        if (user.has("name")){
                            String screenName = user.get("screen_name").getAsString();
                            su.setUserid(screenName);
                            su.setUrl("https://twitter.com/"+screenName);
                        }

                        MediaItem item = mediaDao.getMediaItem(tweetId);
                        if (item != null) {
                            System.out.println(item);
                            item.setUserId(userId);
                            System.out.println(user);
                            //mediaDao.updateMediaItem(item);
                            userDAO.updateStreamUser(su);
                        }
                    }
                } catch (Exception ex) {

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
                MediaItem item = mediaDao.getMediaItem(tweetId);
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
