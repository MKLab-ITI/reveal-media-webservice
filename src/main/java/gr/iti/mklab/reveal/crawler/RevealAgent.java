package gr.iti.mklab.reveal.crawler;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import gr.iti.mklab.reveal.configuration.Configuration;
import gr.iti.mklab.reveal.visual.VisualIndexer;
import gr.iti.mklab.simmo.morphia.MorphiaManager;
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

import java.io.File;
import java.nio.charset.Charset;
import java.util.Date;

/**
 * Created by kandreadou on 2/10/15.
 */
public class RevealAgent implements Runnable {

    private final static Logger LOGGER = LoggerFactory.getLogger(RevealAgent.class);
    public final static BloomFilter<String> UNIQUE_IMAGE_URLS = BloomFilter.create(Funnels.stringFunnel(Charset.forName("UTF-8")), 100000);

    private final String _hostname;
    private final int _jmxPort;
    private CrawlRequest _request;
    private Agent agent;
    private DAO<CrawlRequest, ObjectId> dao;
    private VisualIndexer _indexer;

    public RevealAgent(String hostname, int jmxPort, CrawlRequest request) throws Exception {
        _hostname = hostname;
        _jmxPort = jmxPort;
        _request = request;
        _indexer = new VisualIndexer(_request.collectionName);
    }

    @Override
    public void run() {
        try {
            // Mark the request as running
            dao = new BasicDAO<>(CrawlRequest.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB(CrawlQueueController.DB_NAME).getName());
            _request.requestState = CrawlRequest.STATE.RUNNING;
            _request.lastStateChange = new Date();
            dao.save(_request);

            final BaseConfiguration additional = new BaseConfiguration();
            additional.addProperty("name", _request.collectionName);
            additional.addProperty("group", "gr.iti.mklab");
            additional.addProperty("crawlIsNew", _request.isNew);
            additional.addProperty("weight", "1");
            //NOTE: This is new
            additional.addProperty("rootDir", Configuration.CRAWLS_DIR + _request.collectionName);

            LOGGER.warn("###### Starting Agent for request id " + _request.id + " and collection name " + _request.collectionName);
            RuntimeConfiguration rc = new RuntimeConfiguration(new StartupConfiguration("reveal.properties", additional));
            rc.keywords = _request.keywords;
            rc.collectionName = _request.collectionName;
            rc.indexer = _indexer;
            agent = new Agent(_hostname, _jmxPort, rc);
            LOGGER.warn("###### Agent for request id " + _request.id + " finished");
            _request = dao.findOne("_id", _request.id);
            if (_request != null)
                LOGGER.warn("###### Found request with id " + _request.id + " " + _request.requestState);
            if (_request.requestState == CrawlRequest.STATE.DELETING) {
                LOGGER.warn("###### Delete");
                //Delete the request from the request DB
                dao.delete(_request);
                //Delete the collection DB
                MorphiaManager.getDB(_request.collectionName).dropDatabase();
                //Delete the crawl and index folders
                FileUtils.deleteDirectory(new File(_request.crawlDataPath));
                FileUtils.deleteDirectory(new File(Configuration.VISUAL_DIR + _request.collectionName));

            } else {
                LOGGER.warn("###### Cancel");
                _request.requestState = CrawlRequest.STATE.FINISHED;
                _request.lastStateChange = new Date();
                dao.save(_request);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public void stop() {
        agent.stop();
    }

}
