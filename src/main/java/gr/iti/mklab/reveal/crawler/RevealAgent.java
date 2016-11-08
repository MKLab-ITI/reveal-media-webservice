package gr.iti.mklab.reveal.crawler;

import gr.iti.mklab.reveal.crawler.seeds.DogpileSource;
import gr.iti.mklab.reveal.crawler.seeds.SeedURLSource;
import gr.iti.mklab.reveal.entities.IncrementalNeReExtractor;
import gr.iti.mklab.reveal.util.Configuration;
import gr.iti.mklab.reveal.util.StreamManagerClient;
import gr.iti.mklab.simmo.core.jobs.CrawlJob;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import it.unimi.di.law.bubing.Agent;
import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.StartupConfiguration;

import org.apache.commons.configuration.BaseConfiguration;
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

import java.util.Date;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by kandreadou on 2/10/15.
 */
public class RevealAgent implements Runnable {

    private final static Logger LOGGER = LoggerFactory.getLogger(RevealAgent.class);

    private final String _hostname;
    private final int _jmxPort;
    
    private final StreamManagerClient _manager;
    private CrawlJob _request;
    private DAO<CrawlJob, ObjectId> dao;

    private VisualIndexer visualIndexer = null;
    private IncrementalNeReExtractor inereExtractor = null;
    
    private Future<?> visualIndexerHandle = null, inereHandle = null;
    private ExecutorService executorService = Executors.newFixedThreadPool(3);
    
    public RevealAgent(String hostname, int jmxPort, CrawlJob request, StreamManagerClient manager) {
    	LOGGER.info("RevealAgent constructor for hostname " + hostname);
        _hostname = hostname;
        _jmxPort = jmxPort;
        _request = request;
        _manager = manager;
    }
	
    @Override
    public void run() {
        try {
            LOGGER.info("###### REVEAL agent run method for collection " + _request.getCollection());
            
            dao = new BasicDAO<>(CrawlJob.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getCrawlsDB().getName());           
            visualIndexer = new VisualIndexer(_request.getCollection());
            visualIndexerHandle = executorService.submit(visualIndexer);
            
            if(!visualIndexer.isRunning()) {
            	LOGGER.error("Visual Indexer failed to start for " + _request.getCollection());
            }
            
            inereExtractor = new IncrementalNeReExtractor(_request.getCollection());
            inereHandle = executorService.submit(inereExtractor);
            
            if (Configuration.ADD_SOCIAL_MEDIA) {
            	try {
            		_manager.addAllKeywordFeeds(_request.getKeywords(), _request.getCollection());
            	}
            	catch(Exception e) {
            		LOGGER.error("Failed to add keywords in stream manager for " + _request.getCollection() , e);
            	}	
            }
            
            // Mark the request as running
            _request.setState(CrawlJob.STATE.RUNNING);
            _request.setLastStateChange(new Date());
            dao.save(_request);

            final BaseConfiguration additional = new BaseConfiguration();
            additional.addProperty("name", _request.getCollection());
            additional.addProperty("group", "gr.iti.mklab");
            additional.addProperty("crawlIsNew", _request.isNew());
            additional.addProperty("weight", "1");
            additional.addProperty("rootDir", Configuration.CRAWLS_DIR + _request.getCollection());
            
            //Add the dog-pile links
    		SeedURLSource dogpile = new DogpileSource();
    		Set<String> dogpileUrls = dogpile.getSeedURLs(_request.getKeywords());

            LOGGER.info("###### Starting Agent for request id " + _request.getId() + " and collection " + _request.getCollection());
            RuntimeConfiguration rc = new RuntimeConfiguration(new StartupConfiguration("reveal.properties", additional), dogpileUrls);
            rc.keywords = _request.getKeywords();
            rc.collectionName = _request.getCollection();
            
            LOGGER.info("###### Agent for collection " + _request.getCollection() + " started");
            new Agent(_hostname, _jmxPort, rc);	// agent halts here    
            LOGGER.info("###### Agent for collection " + _request.getCollection() + " finished");
            
            LOGGER.info("###### unregister bean for " + _request.getCollection());
            unregisterBean(_request.getCollection());
            
            stopServices();
            
            // STOP or KILL 
            if(_request.getState() == CrawlJob.STATE.KILLING || _request.getState() == CrawlJob.STATE.STOPPING) {
            	LOGGER.info("###### Stop " + _request.getCollection());
                _request.setState(CrawlJob.STATE.FINISHED);
                _request.setLastStateChange(new Date());
                dao.save(_request);
            }
            
        } catch (Exception e) {
            LOGGER.error("Exception for collection " + _request.getCollection() + ". Message: "+ e.getMessage(), e);
        }
    }

    public void stopServices() {
        
    	LOGGER.info("###### stop indexing runner and social media crawler for " + _request.getCollection());
    	visualIndexer.stop();
        visualIndexerHandle.cancel(true);
        
        inereHandle.cancel(true);

        if (Configuration.ADD_SOCIAL_MEDIA) {
            _manager.deleteAllFeeds(false, _request.getCollection());
        }
    }
    
    /**
     * Stops the BUbiNG Agent listening to the specified port
     */
    public void stop() {
    	LOGGER.info("Cancel BUbiNG Agent for " + _request.getCollection());
    	try {
    		JMXServiceURL jmxServiceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://127.0.0.1:9999/jmxrmi");
    		JMXConnector cc = JMXConnectorFactory.connect(jmxServiceURL);
    		MBeanServerConnection mbsc = cc.getMBeanServerConnection();
                
    		//This information is available in jconsole
    		ObjectName serviceConfigName = new ObjectName("it.unimi.di.law.bubing:type=Agent,name=" + _request.getCollection());
                
    		// Invoke stop operation
    		mbsc.invoke(serviceConfigName, "stop", null, null);
                
    		// Close JMX connector
            cc.close();
    	} catch (Exception e) {
    		LOGGER.error("Exception occurred for " + _request.getCollection(), e);
    	}
    }
    
    private void unregisterBean(String name) {
        try {
        	JMXServiceURL jmxServiceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://127.0.0.1:9999/jmxrmi");
            JMXConnector cc = JMXConnectorFactory.connect(jmxServiceURL);
            MBeanServerConnection mbsc = cc.getMBeanServerConnection();
            
            //This information is available in jconsole
            ObjectName serviceConfigName = new ObjectName("it.unimi.di.law.bubing:type=Agent,name=" + name);
            mbsc.unregisterMBean(serviceConfigName);
            
            // Close JMX connector
            cc.close();
        } catch (Exception e) {
        	LOGGER.error("Exception occurred: " + e.toString());
            e.printStackTrace();
        }
    }

}
