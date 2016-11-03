package gr.iti.mklab.reveal.crawler;

import gr.iti.mklab.reveal.crawler.seeds.DogpileSource;
import gr.iti.mklab.reveal.crawler.seeds.SeedURLSource;
import gr.iti.mklab.reveal.entitites.NEandRECallable;
import gr.iti.mklab.reveal.summarization.MediaSummarizer;
import gr.iti.mklab.reveal.util.Configuration;
import gr.iti.mklab.reveal.visual.VisualIndexClient;
import gr.iti.mklab.simmo.core.jobs.CrawlJob;
import gr.iti.mklab.simmo.core.jobs.Job.STATE;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import it.unimi.di.law.bubing.Agent;
import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.StartupConfiguration;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.io.FileUtils;
import org.bson.types.ObjectId;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.io.File;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by kandreadou on 2/10/15.
 */
public class RevealAgent implements Callable<STATE> {

    private final static Logger LOGGER = LoggerFactory.getLogger(RevealAgent.class);

    private final String _hostname;
    private final int _jmxPort;
    
    private final StreamManagerClient _manager;
    private CrawlJob _request;
    private DAO<CrawlJob, ObjectId> dao;
    
    private Agent _bubingAgent;	// Web crawler
    private VisualIndexer runner = null;
    
    public RevealAgent(String hostname, int jmxPort, CrawlJob request, StreamManagerClient manager) {
    	LOGGER.info("RevealAgent constructor for hostname " + hostname);
        _hostname = hostname;
        _jmxPort = jmxPort;
        _request = request;
        _manager = manager;
    }
	
    @Override
    public STATE call() {
        try {
            LOGGER.info("###### REVEAL agent call method for collection " + _request.getCollection());
            
            // Mark the request as running
            dao = new BasicDAO<>(CrawlJob.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getCrawlsDB().getName());           
            runner = new VisualIndexer(_request.getCollection());

            Thread indexingThread = new Thread(runner);
            indexingThread.start();
            
            LOGGER.info("###### After the indexing runner has been created");
            if (Configuration.ADD_SOCIAL_MEDIA) {
            	try {
            		_manager.addAllKeywordFeeds(_request.getKeywords(), _request.getCollection());
            	}
            	catch(Exception e) {
            		LOGGER.error("Failed to add keywords in stream manager for " + _request.getCollection() , e);
            	}	
            }
            
            _request.setState(CrawlJob.STATE.RUNNING);
            _request.setLastStateChange(new Date());
            dao.save(_request);

            final BaseConfiguration additional = new BaseConfiguration();
            additional.addProperty("name", _request.getCollection());
            additional.addProperty("group", "gr.iti.mklab");
            additional.addProperty("crawlIsNew", _request.isNew());
            additional.addProperty("weight", "1");
            //NOTE: This is new
            additional.addProperty("rootDir", Configuration.CRAWLS_DIR + _request.getCollection());
            //Add the dogpile links
    		SeedURLSource dogpile = new DogpileSource();
    		Set<String> dogpileUrls = dogpile.getSeedURLs(_request.getKeywords());
            //additional.addProperty("dogpile", dogpileUrls.toArray(new String[dogpileUrls.size()]));

            LOGGER.info("###### Starting Agent for request id " + _request.getId() + " and collection name " + _request.getCollection());
            RuntimeConfiguration rc = new RuntimeConfiguration(new StartupConfiguration("reveal.properties", additional), dogpileUrls);
            rc.keywords = _request.getKeywords();
            rc.collectionName = _request.getCollection();
            LOGGER.info("###### Agent for collection " + _request.getCollection() + " started");
            _bubingAgent = new Agent(_hostname, _jmxPort, rc);	// agent halts here
            
            LOGGER.info("###### Agent for collection " + _request.getCollection() + " finished");
            _request = dao.findOne("_id", _request.getId());
            
            if (_request != null) {
                LOGGER.info("###### Found request with id " + _request.getId() + " " + _request.getState());
            }
            else {
            	LOGGER.error("Could not find a saved Crawl Job");
            }
            
            if (_request.getState() == CrawlJob.STATE.DELETING) {
                LOGGER.info("###### Delete " + _request.getCollection());
                //Delete the request from the request DB
                dao.delete(_request);
                //Delete the collection DB
                MorphiaManager.getDB(_request.getCollection()).dropDatabase();
                
                LOGGER.info("###### stop indexing runner for " + _request.getCollection());
                runner.stop();
                indexingThread.interrupt();
                
                //Unload from memory
                String indexServiceHost = "http://" + Configuration.INDEX_SERVICE_HOST + ":8080/VisualIndexService";
        		VisualIndexClient vIndexClient = new VisualIndexClient(indexServiceHost, _request.getCollection());        		
        		vIndexClient.removeCollection();
        		
                //Delete the crawl and index folders
                FileUtils.deleteDirectory(new File(_request.getCrawlDataPath()));
                FileUtils.deleteDirectory(new File(Configuration.VISUAL_DIR + _request.getCollection()));
            
                LOGGER.info("###### stop  social media crawler for " + _request.getCollection());
                if (Configuration.ADD_SOCIAL_MEDIA) {
                    _manager.deleteAllFeeds(false, _request.getCollection());
                }
            } 
            else if(_request.getState() == CrawlJob.STATE.KILLING) {
                LOGGER.info("###### Kill " + _request.getCollection());
                _request.setState(CrawlJob.STATE.FINISHED);
                _request.setLastStateChange(new Date());
                dao.save(_request);
                LOGGER.info("###### stop indexing runner and social media crawler");
                runner.stop();
                indexingThread.interrupt();
                if (Configuration.ADD_SOCIAL_MEDIA) {
                    _manager.deleteAllFeeds(false, _request.getCollection());
                }
            }
            else {
                //STOPPING state
                LOGGER.info("###### Stop " + _request.getCollection() + ". State = " + _request.getState().name());
                
                LOGGER.info("###### stop  social media crawler for " + _request.getCollection());
                if (Configuration.ADD_SOCIAL_MEDIA) {
                    _manager.deleteAllFeeds(false, _request.getCollection());
                }
                
                LOGGER.info("###### stop indexing runner for " + _request.getCollection());
                runner.stop();
                indexingThread.interrupt();
                
                
                LOGGER.info("###### Extract entities for " + _request.getCollection());
                //extract entities for the collection
                ExecutorService entitiesExecutor = Executors.newSingleThreadExecutor();
                entitiesExecutor.submit(new NEandRECallable(_request.getCollection()));
                
                LOGGER.info("###### Summarization of " + _request.getCollection());
                // summarization of the collection
                ExecutorService clusteringExecutor = Executors.newSingleThreadExecutor();
                clusteringExecutor.submit( new MediaSummarizer(_request.getCollection(), 0.65, 0.25, 0.75, 4, 0.7));
                
                _request.setState(CrawlJob.STATE.FINISHED);
                _request.setLastStateChange(new Date());
                dao.save(_request);
            }
            
            LOGGER.warn("###### unregister bean for name");
            unregisterBeanForName(_request.getCollection());
        } catch (Exception e) {
            LOGGER.error("Exception for collection " + _request.getCollection() + ". Message: "+ e.getMessage(), e);
        }
        
        return _request.getState();
    }

    public void stop() {
    	_bubingAgent.stop();
    }

    private void unregisterBeanForName(String name) {
        try {
        	//JMXServiceURL jmxServiceURL = new JMXServiceURL("service:jmx:rmi://localhost/jndi/rmi://localhost:9999/jmxrmi");
            JMXServiceURL jmxServiceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://127.0.0.1:9999/jmxrmi");
            JMXConnector cc = JMXConnectorFactory.connect(jmxServiceURL);
            MBeanServerConnection mbsc = cc.getMBeanServerConnection();
            //This information is available in jconsole
            ObjectName serviceConfigName = new ObjectName("it.unimi.di.law.bubing:type=Agent,name=" + name);
            mbsc.unregisterMBean(serviceConfigName);
            // Close JMX connector
            cc.close();
        } catch (Exception e) {
            System.out.println("Exception occurred: " + e.toString());
            e.printStackTrace();
        }
    }

}
