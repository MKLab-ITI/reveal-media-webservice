package gr.iti.mklab.reveal.entitites;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import gr.demokritos.iit.api.API;
import gr.demokritos.iit.ner.NamedEntityList;
import gr.demokritos.iit.re.RelationCounter;
import gr.iti.mklab.reveal.text.NameThatEntity;
import gr.iti.mklab.reveal.text.TextPreprocessing;
import gr.iti.mklab.simmo.core.annotations.NamedEntity;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.morphia.MediaDAO;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import org.apache.axis.utils.StringUtils;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by kandreadou on 6/4/15.
 */
public class EntitiesExtractionCallable implements Callable<List<NamedEntity>> {

    private String collection;

    /**
     * A Multiset to store frequencies for named entities
     */
    public Multiset<String> ENTITIES_MULTISET = ConcurrentHashMultiset.create();
    /**
     * A HashMap to store entity strings and tokens
     */
    private Map<String, String> ENTITIES_MAP = new HashMap<>();

    private DAO<NamedEntity, String> entitiesDAO;

    private final static int RANKED_ENTITIES_MAX_NUM = 400;

    public EntitiesExtractionCallable(String collection) {
        this.collection = collection;
        entitiesDAO = new BasicDAO<>(NamedEntity.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB(collection).getName());
    }

    @Override
    public List<NamedEntity> call() throws Exception {

        MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, collection);
        List<Image> list = imageDAO.getItems((int)imageDAO.count(), 0);
        HashMap<String, NamedEntityList> REmap = new HashMap<>();

        for(Image i:list){
            if(!StringUtils.isEmpty(i.getTitle())) {
                NamedEntityList entities = API.NER(i.getTitle(), API.FORMAT.TEXT_CERTH_TWEET, false);
                //REmap.put(i.getTitle(), entities);
                for (gr.demokritos.iit.ner.NamedEntity ne : entities) {
                    NamedEntity simmoNE = new NamedEntity(ne.getText(), ne.getType());
                    simmoNE.setId(ne.getID());
                    i.addAnnotation(simmoNE);
                    imageDAO.save(i);
                    if (ENTITIES_MULTISET.add(simmoNE.getToken().toLowerCase()))
                        ENTITIES_MAP.put(simmoNE.getToken().toLowerCase(), ne.getType());
                }
            }
        }

        int entitiesCount = 0;

        Iterable<Multiset.Entry<String>> cases =
                Multisets.copyHighestCountFirst(ENTITIES_MULTISET).entrySet();
        for (Multiset.Entry<String> s : cases) {
            if (entitiesCount > RANKED_ENTITIES_MAX_NUM)
                break;
            NamedEntity y = new NamedEntity(s.getElement(), ENTITIES_MAP.get(s.getElement()), s.getCount());
            entitiesDAO.save(y);
            entitiesCount++;
        }

        //RelationCounter counter = API.RE(REmap, API.FORMAT.TEXT_CERTH_TWEET);
        //counter.prettyPrint();
        return null;
    }

    public static void main(String[] args) throws Exception {
        //Configuration.load("local.properties");
        MorphiaManager.setup("127.0.0.1");
        //NameThatEntity nte = new NameThatEntity();
        //nte.initPipeline();
        ExecutorService clusteringExecutor = Executors.newSingleThreadExecutor();
        clusteringExecutor.submit(new EntitiesExtractionCallable("earthquake")).get();
        clusteringExecutor.shutdown();
        MorphiaManager.tearDown();
    }

}
