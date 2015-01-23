package gr.iti.mklab.reveal.mongo;

import com.google.gson.Gson;
import com.mongodb.*;
import com.mongodb.util.JSON;
import eu.socialsensor.framework.client.dao.impl.MediaClusterDAOImpl;

import eu.socialsensor.framework.client.mongo.MongoHandler;
import gr.iti.mklab.reveal.util.MediaCluster;
import gr.iti.mklab.reveal.util.MediaItem;


import java.lang.reflect.Field;
import java.util.ArrayList;

import java.util.List;

/**
 * Created by kandreadou on 10/7/14.
 */
public class RevealMediaClusterDaoImpl extends MediaClusterDAOImpl {

    private static Gson gson = new Gson();
    private MongoHandler mongoHandler;
    private DBCollection dbCollection;

    public RevealMediaClusterDaoImpl(String host, String db, String collection) throws Exception {
        super(host, db, collection);
        // Get private mongoHandler field with Reflection
        // This is a temporary solution til the SIMMO framework is ready
        Field privateField = MediaClusterDAOImpl.class.getDeclaredField("mongoHandler");

        //this call allows private fields to be accessed via reflection
        privateField.setAccessible(true);

        //getting value of private field using reflection
        mongoHandler = (MongoHandler) privateField.get(this);
        mongoHandler.sortBy("count", MongoHandler.ASC);

        Field collectionPrivateField = MongoHandler.class.getDeclaredField("collection");
        collectionPrivateField.setAccessible(true);
        dbCollection = (DBCollection) collectionPrivateField.get(mongoHandler);
    }

    public void saveCluster(MediaCluster cluster){
        DBObject dbObject = (DBObject) JSON.parse(cluster.toJSONString());
        dbCollection.insert(dbObject);
    }

    public void teardown(){
        // DO NOT DO CLEAN. It drops the collection
        mongoHandler.close();
    }

    public void updateCluster(String id, int count){
        BasicDBObject newDocument = new BasicDBObject();
        newDocument.append("$set", new BasicDBObject().append("count", count));

        BasicDBObject searchQuery = new BasicDBObject().append("id", id);

        dbCollection.update(searchQuery, newDocument);
    }

    public List<MediaCluster> getSortedClusters(int offset, int limit) {
        DBCursor cursor = dbCollection.find().sort(new BasicDBObject("count", -1)).skip(offset);
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
        List<MediaCluster> mediaClusters = new ArrayList<MediaCluster>(jsonResults.size());
        for (String json : jsonResults) {
            MediaCluster item = gson.fromJson(json, MediaCluster.class);
            mediaClusters.add(item);
        }
        return mediaClusters;
    }

    public MediaCluster getCluster(String id){
        String json = mongoHandler.findOne("id", id);
        return gson.fromJson(json, MediaCluster.class);
    }

    public static void main(String[] args) {
        try {
            int offset = 0;
            int count = 20;
            RevealMediaClusterDaoImpl dao = new RevealMediaClusterDaoImpl("160.40.51.20", "Showcase", "MediaClusters");

            List<MediaCluster> clusters = dao.getSortedClusters(0, 10);
            for (MediaCluster c : clusters) {
                System.out.println(c.toJSONString());
            }
        } catch (Exception ex) {
            System.out.println(ex);
        }
    }
}
