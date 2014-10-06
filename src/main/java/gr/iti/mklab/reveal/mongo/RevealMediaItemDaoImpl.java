package gr.iti.mklab.reveal.mongo;

import com.mongodb.*;
import com.mongodb.util.JSON;
import eu.socialsensor.framework.client.dao.impl.MediaItemDAOImpl;
import eu.socialsensor.framework.client.mongo.MongoHandler;
import eu.socialsensor.framework.client.mongo.Selector;
import eu.socialsensor.framework.common.domain.MediaItem;
import eu.socialsensor.framework.common.factories.ItemFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by kandreadou on 9/19/14.
 */
public class RevealMediaItemDaoImpl extends MediaItemDAOImpl {

    private MongoHandler mongoHandler;
    private DBCollection dbCollection;

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

    public List<MediaItem> search(long publicationDate, long width, long height, boolean indexed) {
        Selector query = new Selector();
        query.selectGreaterThan("publicationTime", publicationDate);
        query.selectGreaterThan("width", width);
        query.selectGreaterThan("height", height);
        query.select("indexed", indexed);
        List<String> results = mongoHandler.findMany(query, 10);
        List<MediaItem> mediaItems = new ArrayList<MediaItem>(results.size());
        for (String json : results) {
            mediaItems.add(ItemFactory.createMediaItem(json));
        }
        return mediaItems;
    }

    public List<MediaItem> getMediaItems(int offset, int limit) {
        DBCursor cursor = dbCollection.find(new BasicDBObject()).skip(offset);
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
            mediaItems.add(ItemFactory.createMediaItem(json));
        }
        return mediaItems;
    }


    public static void main(String[] args) {
        try {
            RevealMediaItemDaoImpl mediaDao = new RevealMediaItemDaoImpl("160.40.51.20", "Showcase", "MediaItems");
            List<MediaItem> items = mediaDao.getMediaItems(3, 4);
            for (MediaItem item : items) {
                System.out.println(item);
            }
        } catch (Exception ex) {
            //ignore
        }
    }
}
