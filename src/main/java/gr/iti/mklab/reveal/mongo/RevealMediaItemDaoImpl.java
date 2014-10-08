package gr.iti.mklab.reveal.mongo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.*;
import com.mongodb.util.JSON;
import eu.socialsensor.framework.client.dao.impl.MediaItemDAOImpl;
import eu.socialsensor.framework.client.dao.impl.StreamUserDAOImpl;
import eu.socialsensor.framework.client.mongo.MongoHandler;
import eu.socialsensor.framework.client.mongo.Selector;
import eu.socialsensor.framework.common.domain.StreamUser;
import eu.socialsensor.framework.common.factories.ItemFactory;
import gr.iti.mklab.reveal.util.MediaItem;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by kandreadou on 9/19/14.
 */
public class RevealMediaItemDaoImpl extends MediaItemDAOImpl {

    static Gson gson = new Gson();

    private MongoHandler mongoHandler;
    private DBCollection dbCollection;
    private StreamUserDAOImpl userDAO = new StreamUserDAOImpl("160.40.51.20", "Showcase", "StreamUsers");

    public RevealMediaItemDaoImpl() throws Exception {
        super("localhost");
    }

    public RevealMediaItemDaoImpl(String mongohost, String db, String collection) throws Exception {
        super(mongohost, db, collection);

        // Get private mongoHandler field with Reflection
        // This is a temporary solution til the SIMMO framework is ready
        Field privateField = MediaItemDAOImpl.class.getDeclaredField("mongoHandler");

        //this call allows private fields to be accessed via reflection
        privateField.setAccessible(true);

        MongoHandler.class.
                getDeclaredMethod("findMany", Selector.class, int.class).setAccessible(true);

        //getting value of private field using reflection
        mongoHandler = (MongoHandler) privateField.get(this);
        mongoHandler.sortBy("id", MongoHandler.ASC);

        Field collectionPrivateField = MongoHandler.class.getDeclaredField("collection");
        collectionPrivateField.setAccessible(true);
        dbCollection = (DBCollection) collectionPrivateField.get(mongoHandler);

    }

    public List<MediaItem> search(String username, String text, int width, int height, long publicationDate, int limit, int offset) {
        BasicDBObject q = new BasicDBObject();
        if (!StringUtils.isEmpty(text))
            q.put("description", java.util.regex.Pattern.compile(text, Pattern.CASE_INSENSITIVE));
        if (!StringUtils.isEmpty(username)){
            DBObject o = new BasicDBObject();
            o.put("userid",username);
            List<StreamUser> users = userDAO.getStreamUsers(o);
            if(users!=null && users.size()>0){
                q.put("uid",users.get(0).getId());
            }else{
                return new ArrayList<MediaItem>();
            }
        }
        if (width > 0)
            q.put("width", new BasicDBObject("$gt", width));
        if (height > 0)
            q.put("height", new BasicDBObject("$gt", height));
        if (publicationDate > 0)
            q.put("publicationTime", new BasicDBObject("$gt", publicationDate));
        return get(q, offset, limit);
    }

    public List<MediaItem> getMediaItems(int offset, int limit) {
        return get(new BasicDBObject(), offset, limit);
    }

    public MediaItem getItem(String id){
        String json = mongoHandler.findOne("id", id);
        return gson.fromJson(json, MediaItem.class);
    }

    private List<MediaItem> get(BasicDBObject object, int offset, int limit) {
        DBCursor cursor = dbCollection.find(object).skip(offset);
        List<String> jsonResults = new ArrayList<String>();
        if (limit > 0) {
            cursor = cursor.limit(limit);
        }
        try {
            while (cursor.hasNext()) {
                DBObject current = cursor.next();
                jsonResults.add(JSON.serialize(current));
            }
        } finally {
            cursor.close();
        }
        List<MediaItem> mediaItems = new ArrayList<MediaItem>(jsonResults.size());
        for (String json : jsonResults) {
            MediaItem item = gson.fromJson(json, MediaItem.class);
            StreamUser user = userDAO.getStreamUser(item.getUserId());
            item.setUser(user);
            mediaItems.add(item);
        }
        return mediaItems;
    }


    public static void main(String[] args) {
        try {
            RevealMediaItemDaoImpl mediaDao = new RevealMediaItemDaoImpl("160.40.51.20", "Showcase", "MediaItems");

            List<MediaItem> items = mediaDao.search("mrsShanegray",null, 0, 0, -1, 10, 0);
            //List<MediaItem> items = mediaDao.getMediaItems(10,50);
            for (MediaItem item : items) {
                System.out.println(item.toJSONString());
            }
        } catch (Exception ex) {
            //ignore
        }
    }
}
