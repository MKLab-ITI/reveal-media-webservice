package gr.iti.mklab.reveal.util;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import com.google.gson.Gson;
import gr.iti.mklab.reveal.mongo.RevealMediaItemDaoImpl;
import gr.iti.mklab.reveal.text.NameThatEntity;
import gr.iti.mklab.reveal.text.TextPreprocessing;
import gr.iti.mklab.simmo.annotations.*;
import gr.iti.mklab.simmo.morphia.MorphiaManager;
import org.bson.types.ObjectId;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by kandreadou on 10/2/14.
 */
public class NamedEntityImporter {

    private final Gson gson = new Gson();

    /**
     * A HashMap to store frequencies for image URLs from specific news domains
     */
    public static Multiset<String> entities = ConcurrentHashMultiset.create();
    private Map<String, String> map = new HashMap<String, String>();

    public static void main(String[] args) throws Exception {
        NamedEntityImporter nei = new NamedEntityImporter();
        nei.extractFromItems();
    }

    public void parse() throws Exception {
        String path = "/home/kandreadou/Pictures/snow_named_entities.json";
        NamedEntityDAO dao = new NamedEntityDAO("160.40.51.20", "Showcase", "NamedEntities");

        BufferedReader br = new BufferedReader(new FileReader(new File(path)));
        String line = "";
        while ((line = br.readLine()) != null) {
            NamedEntities tweet = gson.fromJson(line, NamedEntities.class);
            dao.addItem(tweet);
            //System.out.println(tweet);
        }
    }

    public void extractFromItems() throws Exception {
        MorphiaManager.setup("160.40.51.20");
        NameThatEntity nte = new NameThatEntity();
        nte.initPipeline();
        DAO<NamedEntities, ObjectId> nedao = new BasicDAO<>(NamedEntities.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB("malaysia").getName());
        DAO<NamedEntity, ObjectId> rankedDAO = new BasicDAO<>(NamedEntity.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB("malaysia").getName());
        RevealMediaItemDaoImpl mediaDao = new RevealMediaItemDaoImpl("160.40.51.20", "malaysia", "MediaItems");
        List<MediaItem> list = mediaDao.getMediaItems(0, 25076, null);
        for (MediaItem item : list) {
            if(item.getDescription()!=null || item.getTitle()!=null) {
                TextPreprocessing textPre = new TextPreprocessing(item.getDescription()+" "+item.getTitle());
                // Get the cleaned text
                ArrayList<String> cleanedText = textPre.getCleanedSentences();
                //Run the NER
                List<gr.iti.mklab.simmo.annotations.NamedEntity> names = nte.tagIt(cleanedText);
                NamedEntities ne = new NamedEntities();
                ne.tweetId = item.getId();
                ne.namedEntities = new NamedEntity[names.size()];
                for (int i = 0; i < names.size(); i++) {
                    ne.namedEntities[i] = new NamedEntity(names.get(i).getToken(), names.get(i).getType(), 0);

                    if (entities.add(ne.namedEntities[i].token)) {
                        map.put(ne.namedEntities[i].token, ne.namedEntities[i].type);
                    }
                }
                nedao.save(ne);
            }
        }
        Iterable<Multiset.Entry<String>> cases =
                Multisets.copyHighestCountFirst(entities).entrySet();
        for (Multiset.Entry<String> s : cases) {
            if (s.getCount() < 10)
                break;
            NamedEntity y = new NamedEntity(s.getElement(), map.get(s.getElement()), s.getCount());
            rankedDAO.save(y);
            System.out.println(s.getElement() + " count " + s.getCount());

        }
        MorphiaManager.tearDown();

    }

}
