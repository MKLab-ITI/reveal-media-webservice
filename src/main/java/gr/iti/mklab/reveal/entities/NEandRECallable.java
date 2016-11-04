package gr.iti.mklab.reveal.entities;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;


import gr.demokritos.iit.api.API;
import gr.demokritos.iit.ner.NamedEntityList;
import gr.demokritos.iit.re.Relation;
import gr.demokritos.iit.re.RelationCounter;
import gr.demokritos.iit.re.RelationList;
import gr.iti.mklab.simmo.core.Association;
import gr.iti.mklab.simmo.core.annotations.NamedEntity;
import gr.iti.mklab.simmo.core.associations.TextualRelation;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.morphia.MediaDAO;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import org.apache.axis.utils.StringUtils;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by kandreadou on 6/4/15.
 */
public class NEandRECallable implements Callable<List<NamedEntity>> {

    private String collection;

    /**
     * A Multiset to store frequencies for named entities
     */
    public Multiset<String> ENTITIES_MULTISET = ConcurrentHashMultiset.create();
    /**
     * A HashMap to store entity strings and tokens
     */
    private Map<String, NamedEntity> ENTITIES_MAP = new HashMap<>();

    /**
     * A Multiset to store frequencies for relations
     */
    public Multiset<String> RELATIONS_MULTISET = ConcurrentHashMultiset.create();
    /**
     * A HashMap to store relation
     */
    private Map<String, Relation> RELATIONS_MAP = new HashMap<>();

    private DAO<NamedEntity, String> entitiesDAO;
    private DAO<Association, String> associationDAO;

    //private final static int RANKED_ENTITIES_MAX_NUM = 400;

    public NEandRECallable(String collection) {
        this.collection = collection;
        entitiesDAO = new BasicDAO<>(NamedEntity.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB(collection).getName());
        associationDAO = new BasicDAO<>(Association.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB(collection).getName());
    }

    @Override
    public List<NamedEntity> call() throws Exception {

        MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, collection);
        List<Image> list = imageDAO.getItems((int) imageDAO.count(), 0);
        //List<Image> list = imageDAO.getItems(1000, 0);
        HashMap<String, NamedEntityList> REmap = new HashMap<>();

        for (Image i : list) {
            if (!StringUtils.isEmpty(i.getTitle())) {
                NamedEntityList entities = API.NER(i.getTitle(), API.FORMAT.TEXT_CERTH_TWEET, false);
                REmap.put(i.getTitle(), entities);
                for (gr.demokritos.iit.ner.NamedEntity ne : entities) {
                    NamedEntity simmoNE = new NamedEntity(ne.getText(), ne.getType());
                    simmoNE.setId(ne.getID());
                    i.addAnnotation(simmoNE);
                    imageDAO.save(i);
                    if (ENTITIES_MULTISET.add(simmoNE.getToken().toLowerCase()))
                        ENTITIES_MAP.put(simmoNE.getToken().toLowerCase(), simmoNE);
                }
            }
        }

        //int entitiesCount = 0;

        Iterable<Multiset.Entry<String>> cases =
                Multisets.copyHighestCountFirst(ENTITIES_MULTISET).entrySet();
        for (Multiset.Entry<String> s : cases) {
            //if (entitiesCount > RANKED_ENTITIES_MAX_NUM)
            //break;
            NamedEntity y = ENTITIES_MAP.get(s.getElement());
            y.setCount(s.getCount());
            entitiesDAO.save(y);
            //entitiesCount++;
        }

        RelationCounter counter = API.RE(REmap, API.FORMAT.TEXT_CERTH_TWEET);

        System.out.println("### RELATIONS COUNT ### " + counter.getGroups().size());
        for (RelationList rl : counter.getGroups().values()) {
            for (Relation r : rl) {
                if (entitiesDAO.exists("_id", r.getSubject().getID()) && entitiesDAO.exists("_id", r.getArgument().getID())) {
                    int count = counter.getCount(r.getLabel());
                    RELATIONS_MULTISET.add(r.getLabel(), count);
                    RELATIONS_MAP.put(r.getLabel(), r);

                }
            }
        }

        Iterable<Multiset.Entry<String>> rankedRelations =
                Multisets.copyHighestCountFirst(RELATIONS_MULTISET).entrySet();
        for (Multiset.Entry<String> s : rankedRelations) {
            Relation r = RELATIONS_MAP.get(s.getElement());
            gr.demokritos.iit.ner.NamedEntity subject = r.getSubject();
            gr.demokritos.iit.ner.NamedEntity argument = r.getArgument();
            String relation = r.getRelationText();
            NamedEntity subjectSIMMO = new NamedEntity(subject.getText(), subject.getType());
            subjectSIMMO.setId(subject.getID());
            NamedEntity argumentSIMMO = new NamedEntity(argument.getText(), argument.getType());
            argumentSIMMO.setId(argument.getID());
            TextualRelation textualRelation = new TextualRelation(subjectSIMMO, argumentSIMMO, relation, s.getCount());
            associationDAO.save(textualRelation);
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        //Configuration.load("local.properties");
        MorphiaManager.setup("127.0.0.1");
        //NameThatEntity nte = new NameThatEntity();
        //nte.initPipeline();
        ExecutorService clusteringExecutor = Executors.newSingleThreadExecutor();
        clusteringExecutor.submit(new NEandRECallable("earthquake")).get();
        clusteringExecutor.shutdown();
        MorphiaManager.tearDown();
    }

}
