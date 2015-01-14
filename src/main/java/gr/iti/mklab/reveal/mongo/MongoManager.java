package gr.iti.mklab.reveal.mongo;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by kandreadou on 7/17/14.
 */
public class MongoManager {

    public DBCollection collection;

    public MongoManager(String hostName, String dbName, String collectionName){
        try {
            MongoClient mongoClient = new MongoClient(hostName);
            DB database = mongoClient.getDB(dbName);
            collection = database.getCollection(collectionName);
        }catch( UnknownHostException ex){
        }
    }

    public DBObject findOne(){
        return collection.findOne();
    }

    public List<DBObject> search(){
        int counter = 0;
        List<DBObject> objects = new ArrayList<DBObject>();

        BasicDBObject query = new BasicDBObject("width", new BasicDBObject("$gt", 3000));

        DBCursor cursor = collection.find(query);
        try {
            while (cursor.hasNext() && counter<50) {
                counter++;
                objects.add(cursor.next());
            }
        } finally {
            cursor.close();
        }
        return objects;
    }
}
