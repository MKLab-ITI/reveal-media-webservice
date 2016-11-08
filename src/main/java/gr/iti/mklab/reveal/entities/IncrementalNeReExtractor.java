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
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.QueryResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by kandreadou on 6/4/15.
 */
public class IncrementalNeReExtractor implements Runnable {

	private final static Logger LOGGER = LoggerFactory.getLogger(IncrementalNeReExtractor.class);
	
    private String _collection;

    /**
     * A Multiset to store frequencies for named entities
     */
    public Multiset<String> ENTITIES = ConcurrentHashMultiset.create();
    
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

    private DAO<NamedEntity, String> _entitiesDAO;
    private DAO<Association, String> _associationDAO;

    private Date since = new Date(0L);

	private boolean isRunning = true;
   
    public IncrementalNeReExtractor(String collection) {
        _collection = collection;
        
        _entitiesDAO = new BasicDAO<>(NamedEntity.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB(collection).getName());
        _associationDAO = new BasicDAO<>(Association.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB(collection).getName());
    }

    public void stop() {
    	isRunning = false;
    }
    
    @Override
    public void run() {

    	while(isRunning) {
    		
    		try {
				Thread.sleep(60000L);
			} catch (InterruptedException e) {
				LOGGER.info("NeRe extractor interrupted for " + _collection, e);
				if(!isRunning) {
					break;
				}
			}
    		
    		Date until = new Date();
        
    		MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, _collection);
    		Query<Image> q = imageDAO.createQuery();
    		q.filter("crawlDate >", since);
    		q.filter("crawlDate <=", until);
        
    		QueryResults<Image> result = imageDAO.find(q);
    		List<Image> images = result.asList();
		
    		since.setTime(until.getTime());
		 
    		HashMap<String, NamedEntityList> REmap = new HashMap<>();
    		for (Image image : images) {
    			if (!StringUtils.isEmpty(image.getTitle())) {
    				NamedEntityList entities = API.NER(image.getTitle(), API.FORMAT.TEXT_CERTH_TWEET, false);
    				REmap.put(image.getTitle(), entities);
    				for (gr.demokritos.iit.ner.NamedEntity ne : entities) {
    					NamedEntity namedEntity = new NamedEntity(ne.getText(), ne.getType());
    					namedEntity.setId(ne.getID());
    					image.addAnnotation(namedEntity);
    					if (ENTITIES.add(namedEntity.getToken().toLowerCase())) {
    						ENTITIES_MAP.put(namedEntity.getToken().toLowerCase(), namedEntity);
    					}
    				}
    				imageDAO.save(image);
    			}
    		}
    		
    		Iterable<Multiset.Entry<String>> cases = Multisets.copyHighestCountFirst(ENTITIES).entrySet();
    		for (Multiset.Entry<String> s : cases) {
    			NamedEntity namedEntity = ENTITIES_MAP.get(s.getElement());
    			namedEntity.setCount(s.getCount());
    			_entitiesDAO.save(namedEntity);
    		}

        	RelationCounter counter = API.RE(REmap, API.FORMAT.TEXT_CERTH_TWEET);
        	LOGGER.info("### RELATIONS COUNT ### " + counter.getGroups().size());
        	for (RelationList rl : counter.getGroups().values()) {
            	for (Relation r : rl) {
                	if (_entitiesDAO.exists("_id", r.getSubject().getID()) && _entitiesDAO.exists("_id", r.getArgument().getID())) {
                    	int count = counter.getCount(r.getLabel());
                    	RELATIONS_MULTISET.add(r.getLabel(), count);
                    	RELATIONS_MAP.put(r.getLabel(), r);
                	}
            	}
        	}

        	Iterable<Multiset.Entry<String>> rankedRelations = Multisets.copyHighestCountFirst(RELATIONS_MULTISET).entrySet();
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
            
            	_associationDAO.save(textualRelation);
        	}
    	}
    }
    
}
