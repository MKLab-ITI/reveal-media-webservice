package gr.iti.mklab.reveal.crawler;

import gr.iti.mklab.reveal.util.Configuration;
import gr.iti.mklab.simmo.core.jobs.CrawlJob;
import gr.iti.mklab.simmo.core.jobs.Job;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import gr.iti.mklab.sm.StreamsManager2;
import gr.iti.mklab.sm.streams.StreamException;
import gr.iti.mklab.sm.streams.StreamsManagerConfiguration;
import org.bson.types.ObjectId;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;

import java.io.File;

/**
 * A geo-based crawling using the StreamManager to crawl Panoramio,
 * Google StreetView and Wikimapia and the IndexingRunner for indexing
 *
 * @author kandreadou
 */
public class GeoCrawler {

    private StreamsManager2 manager;
    private IndexingRunner runner;
    private DAO<CrawlJob, ObjectId> dao;
    private CrawlJob req;

    public GeoCrawler(CrawlJob req) throws Exception {
        dao = new BasicDAO<>(CrawlJob.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getCrawlsDB().getName());
        StreamsManagerConfiguration config = StreamsManagerConfiguration.readFromFile(new File(Configuration.GEO_CONF_FILE));
        manager = new StreamsManager2(config);
        config.getStorageConfig("Mongodb").setParameter("mongodb.database", req.getCollection());
        manager.open(req.getLon_min(), req.getLat_min(), req.getLon_max(), req.getLat_max());
        new Thread(manager).start();
        runner = new IndexingRunner(req.getCollection());
        new Thread(runner).start();
        req.setState(Job.STATE.RUNNING);
        this.req = req;
        dao.save(req);
    }

    public void stop() throws StreamException {
        manager.close();
        runner.stopWhenFinished();
        req.setState(Job.STATE.FINISHED);
        dao.save(req);
    }
}
