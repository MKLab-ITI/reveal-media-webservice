package gr.iti.mklab.reveal.crawler;

import gr.iti.mklab.reveal.crawler.seeds.DogpileSource;
import gr.iti.mklab.reveal.crawler.seeds.SeedURLSource;
import gr.iti.mklab.reveal.entities.IncrementalNeReExtractor;
import gr.iti.mklab.reveal.util.Configuration;
import gr.iti.mklab.reveal.util.StreamManagerClient;
import gr.iti.mklab.reveal.visual.VisualIndexer;
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
import java.util.concurrent.Callable;
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
    private Future<Boolean> bubbingHandle = null;
    
    private ExecutorService executorService = Executors.newFixedThreadPool(4);
    
    
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
            LOGGER.info("Reveal agent run method for collection " + _request.getCollection());
            
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
           
            bubbingHandle = startBubingAgent();
            if(bubbingHandle.isDone() || bubbingHandle.isCancelled()) {
            	LOGGER.error("Bubing Agent failed to start for " + _request.getCollection());
            }
            
            // Mark the request as running
            _request.setState(CrawlJob.STATE.RUNNING);
            _request.setLastStateChange(new Date());
            dao.save(_request);
           
            while(true) {
            	if(!visualIndexerHandle.isDone() && !visualIndexerHandle.isCancelled()) {
                	LOGGER.info("Visual Indexer is running porperly for " + _request.getCollection());
                }
            	
            	if(!inereHandle.isDone() && !inereHandle.isCancelled()) {
                	LOGGER.info("NeRe Extractor is running porperly for " + _request.getCollection());
                }
            	
            	if(!bubbingHandle.isDone() && !bubbingHandle.isCancelled()) {
                	LOGGER.info("Bubbing Agent thread is running porperly for " + _request.getCollection());
                }
            	Thread.sleep(1800000L);
            }
            
        } catch (Exception e) {
            LOGGER.error("Exception for collection " + _request.getCollection() + ". Message: "+ e.getMessage(), e);
        }
    }
    
    public Future<Boolean> startBubingAgent() {
    	
    	return executorService.submit( new Callable<Boolean>() {
			@Override
			public Boolean call() {
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
		        RuntimeConfiguration rc;
				try {
					rc = new RuntimeConfiguration(new StartupConfiguration("reveal.properties", additional), dogpileUrls);
					
			        rc.keywords = _request.getKeywords();
			        rc.collectionName = _request.getCollection();
			        
			        new Agent(_hostname, _jmxPort, rc);	// agent halts here    
			        LOGGER.info("###### Agent for collection " + _request.getCollection() + " finished");

			        LOGGER.info("###### unregister bean for " + _request.getCollection());
			        unregisterBean(_request.getCollection());
			        
			        return true;
				} catch (Exception e) {
					return false;
				}

			}
    		
    	});
    }
    
    /**
     * Stops the BUbiNG Agent listening to the specified port
     */
    public void stopBubingAgent() {
    	try {
    		LOGGER.info("Cancel BUbiNG Agent for " + _request.getCollection());
    		
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
    		LOGGER.error("Exception occurred during stoping of BUbiNG Agent for " + _request.getCollection(), e);
    	}
    }
    
    public void stopServices() {

    	LOGGER.info("Stop indexing runner, NeRe extractor and social media crawler for " + _request.getCollection());
    	visualIndexer.stop();
        boolean canceled = visualIndexerHandle.cancel(true);
        if(!canceled) {
        	LOGGER.error("Visual indexer failed to stop");
        }
        
        inereExtractor.stop();
        canceled = inereHandle.cancel(true);
        if(!canceled) {
        	LOGGER.error("NE and RE extractor failed to stop");
        }
        
        canceled = bubbingHandle.cancel(true);
        if(!canceled) {
        	LOGGER.error("bubbing agent thread failed to stop");
        }
        
        if (Configuration.ADD_SOCIAL_MEDIA) {
            _manager.deleteAllFeeds(false, _request.getCollection());
        }
  
    }
    
    public void stop() {
    	
    	_request = dao.findOne("_id", _request.getId());
    	
    	stopBubingAgent();
    	stopServices();
        
        // STOP or KILL 
        if(_request.getState() == CrawlJob.STATE.KILLING || _request.getState() == CrawlJob.STATE.STOPPING) {
        	LOGGER.info("Stop " + _request.getCollection() + " was successfull.");
            _request.setState(CrawlJob.STATE.FINISHED);
            _request.setLastStateChange(new Date());
            dao.save(_request);
        }
        else {
        	LOGGER.error("State of " + _request + " is " + _request.getState() + " instead of STOPPING / KILLING.");
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
