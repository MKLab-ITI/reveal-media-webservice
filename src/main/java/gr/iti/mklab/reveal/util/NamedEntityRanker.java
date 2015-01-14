package gr.iti.mklab.reveal.util;


import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import gr.iti.mklab.reveal.mongo.RevealMediaItemDaoImpl;
import gr.iti.mklab.simmo.morphia.MorphiaManager;
import org.apache.commons.lang.ArrayUtils;
import org.bson.types.ObjectId;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;

import java.util.*;

/**
 * Created by kandreadou on 1/14/15.
 */
public class NamedEntityRanker {

    /**
     * A HashMap to store frequencies for image URLs from specific news domains
     */
    public static Multiset<String> entities = ConcurrentHashMultiset.create();
    private Map<String, String> map = new HashMap<String, String>();


    public static void main(String[] args) throws Exception {
        NamedEntityRanker r = new NamedEntityRanker();
        r.test();
    }

    private void test() throws Exception {
        MorphiaManager.setup("Showcase", "160.40.51.20");
        RevealMediaItemDaoImpl mediaDao = new RevealMediaItemDaoImpl("localhost", "Showcase", "MediaItems");
        int index = 0;
        List<MediaItem> list = new ArrayList<>();
        DAO<NamedEntities, ObjectId> rankedDAO = new BasicDAO<>(NamedEntities.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB().getName());
        Iterator<NamedEntities> ne = rankedDAO.find().iterator();
        while (ne.hasNext()) {
            index++;
            NamedEntities entities = ne.next();
            if (entities.namedEntities != null) {
                for (NamedEntity y : entities.namedEntities) {
                    if (y.token.equalsIgnoreCase("Obama")) {
                        MediaItem me = mediaDao.getItem(entities.tweetId);
                        if(me!=null){
                            System.out.println("me not null");
                        }
                        list.add(mediaDao.getItem(entities.tweetId));
                        //System.out.println("contains");
                        break;
                    }
                }
            }
        }
        MorphiaManager.tearDown();
    }

    private void rankThem() throws Exception {
        int counter = 0;
        MorphiaManager.setup("Showcase", "160.40.51.20");
        DAO<NamedEntities, ObjectId> dao = new BasicDAO<>(NamedEntities.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB().getName());
        DAO<NamedEntity, ObjectId> rankedDAO = new BasicDAO<>(NamedEntity.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB().getName());
        Iterator<NamedEntities> it = dao.find().iterator();
        while (it.hasNext()) {
            counter++;
            NamedEntities ne = it.next();
            if (ne.namedEntities != null) {
                for (NamedEntity n : ne.namedEntities) {
                    if (entities.add(n.token)) {
                        map.put(n.token, n.type);
                    }
                }
            }

        }
        Iterable<Multiset.Entry<String>> cases =
                Multisets.copyHighestCountFirst(entities).entrySet();
        for (Multiset.Entry<String> s : cases) {
            if (s.getCount() < 100)
                break;
            NamedEntity y = new NamedEntity(s.getElement(), map.get(s.getElement()), s.getCount());
            rankedDAO.save(y);
            System.out.println(s.getElement() + " count " + s.getCount());

        }
        MorphiaManager.tearDown();
    }
}
