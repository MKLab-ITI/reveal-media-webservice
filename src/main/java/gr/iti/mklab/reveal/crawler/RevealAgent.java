package gr.iti.mklab.reveal.crawler;

import gr.iti.mklab.reveal.util.Configuration;
import gr.iti.mklab.simmo.core.jobs.CrawlJob;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import gr.iti.mklab.sm.StreamsManager2;
import gr.iti.mklab.sm.streams.StreamsManagerConfiguration;
import it.unimi.di.law.bubing.Agent;
import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.StartupConfiguration;
import jdk.nashorn.internal.runtime.regexp.joni.Config;
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

/**
 * Created by kandreadou on 2/10/15.
 */
public class RevealAgent implements Runnable {

    private final static Logger LOGGER = LoggerFactory.getLogger(RevealAgent.class);

    private final String _hostname;
    private final int _jmxPort;
    private CrawlJob _request;
    private DAO<CrawlJob, ObjectId> dao;

    public RevealAgent(String hostname, int jmxPort, CrawlJob request) {
        System.out.println("RevealAgent constructor for hostname " + hostname);
        _hostname = hostname;
        _jmxPort = jmxPort;
        _request = request;
    }

    @Override
    public void run() {
        try {
            LOGGER.warn("###### REVEAL agent run method");
            System.out.println("###### REVEAL agent run method");
            IndexingRunner runner = new IndexingRunner(_request.getCollection());
            Thread indexingThread = new Thread(runner);
            indexingThread.start();
            LOGGER.warn("###### After the indexing runner has been created");
            System.out.println("###### After the indexing runner has been created");
            StreamsManager2 manager = null;
            Thread socialmediaCrawlerThread = null;
            if (Configuration.ADD_SOCIAL_MEDIA) {
                File streamConfigFile = new File(Configuration.STREAM_CONF_FILE);
                StreamsManagerConfiguration config = StreamsManagerConfiguration.readFromFile(streamConfigFile);
                config.getStorageConfig("Mongodb").setParameter("mongodb.database", _request.getCollection());
                manager = new StreamsManager2(config);
                manager.open(_request.getKeywords());
                socialmediaCrawlerThread = new Thread(manager);
                socialmediaCrawlerThread.start();
            }
            // Mark the request as running
            dao = new BasicDAO<>(CrawlJob.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getCrawlsDB().getName());
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

            LOGGER.warn("###### Starting Agent for request id " + _request.getId() + " and collection name " + _request.getCollection());
            RuntimeConfiguration rc = new RuntimeConfiguration(new StartupConfiguration("reveal.properties", additional));
            rc.keywords = _request.getKeywords();
            rc.collectionName = _request.getCollection();
            LOGGER.warn("###### Agent for request id " + _request.getId() + " started");
            new Agent(_hostname, _jmxPort, rc);
            LOGGER.warn("###### Agent for request id " + _request.getId() + " finished");
            _request = dao.findOne("_id", _request.getId());
            if (_request != null)
                LOGGER.warn("###### Found request with id " + _request.getId() + " " + _request.getState());
            if (_request.getState() == CrawlJob.STATE.DELETING) {
                LOGGER.warn("###### Delete");
                //Delete the request from the request DB
                dao.delete(_request);
                //Delete the collection DB
                MorphiaManager.getDB(_request.getCollection()).dropDatabase();
                //Delete the crawl and index folders
                FileUtils.deleteDirectory(new File(_request.getCrawlDataPath()));
                FileUtils.deleteDirectory(new File(Configuration.VISUAL_DIR + _request.getCollection()));

            } else {
                LOGGER.warn("###### Cancel");
                _request.setState(CrawlJob.STATE.FINISHED);
                _request.setLastStateChange(new Date());
                dao.save(_request);
            }
            LOGGER.warn("###### ston indexing runner and social media crawler");
            runner.stop();
            indexingThread.interrupt();
            if (Configuration.ADD_SOCIAL_MEDIA) {
                manager.close();
                socialmediaCrawlerThread.interrupt();
            }
            LOGGER.warn("###### unregister bean for name");
            unregisterBeanForName(_request.getCollection());
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            System.out.println(e.getMessage());
        }
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
