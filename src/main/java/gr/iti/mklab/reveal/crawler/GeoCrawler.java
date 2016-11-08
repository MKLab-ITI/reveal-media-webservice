package gr.iti.mklab.reveal.crawler;

import gr.iti.mklab.reveal.util.StreamManagerClient;
import gr.iti.mklab.simmo.core.jobs.CrawlJob;
import gr.iti.mklab.simmo.core.jobs.Job;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import gr.iti.mklab.sm.streams.StreamException;
import org.bson.types.ObjectId;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;


/**
 * A geo-based crawling using the StreamManager to crawl Panoramio,
 * Google StreetView and Wikimapia and the IndexingRunner for indexing
 *
 * @author kandreadou
 */
public class GeoCrawler {

    private StreamManagerClient manager;
    private VisualIndexer runner;
    private DAO<CrawlJob, ObjectId> dao;
    private CrawlJob req;

    public GeoCrawler(CrawlJob req, StreamManagerClient manager) throws Exception {
        this.manager = manager;
        dao = new BasicDAO<>(CrawlJob.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getCrawlsDB().getName());
        manager.addAllGeoFeeds(req.getLon_min(), req.getLat_min(), req.getLon_max(), req.getLat_max(), req.getCollection());
        runner = new VisualIndexer(req.getCollection());
        new Thread(runner).start();
        req.setState(Job.STATE.RUNNING);
        this.req = req;
        dao.save(req);
    }

    public void stop() throws StreamException {
        manager.deleteAllFeeds(true, req.getCollection());
        runner.stop();
        req.setState(Job.STATE.FINISHED);
        dao.save(req);
    }
}
