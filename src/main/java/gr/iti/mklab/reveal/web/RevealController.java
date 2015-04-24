package gr.iti.mklab.reveal.web;

import gr.iti.mklab.reveal.configuration.Configuration;
import gr.iti.mklab.reveal.crawler.CrawlQueueController;
import gr.iti.mklab.reveal.text.NameThatEntity;
import gr.iti.mklab.reveal.text.TextPreprocessing;
import gr.iti.mklab.reveal.text.htmlsegmentation.BoilerpipeContentExtraction;
import gr.iti.mklab.reveal.text.htmlsegmentation.Content;

import gr.iti.mklab.reveal.visual.JsonResultSet;
import gr.iti.mklab.reveal.visual.VisualIndexer;
import gr.iti.mklab.reveal.visual.VisualIndexerFactory;
import gr.iti.mklab.simmo.core.annotations.NamedEntity;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.items.Media;
import gr.iti.mklab.simmo.core.items.Video;
import gr.iti.mklab.simmo.core.jobs.CrawlJob;
import gr.iti.mklab.simmo.core.morphia.MediaDAO;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.bson.types.ObjectId;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;
import org.mongodb.morphia.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PreDestroy;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;


@Controller
@RequestMapping("/mmapi")
public class RevealController {


    private static final Logger logger = LoggerFactory.getLogger(RevealController.class);

    protected CrawlQueueController crawlerCtrler;

    protected NameThatEntity nte;

    public RevealController() throws Exception {
        Configuration.load(getClass().getResourceAsStream("/local.properties"));
        MorphiaManager.setup(Configuration.MONGO_HOST);
        VisualIndexer.init();
        crawlerCtrler = new CrawlQueueController();
        nte = new NameThatEntity();
        nte.initPipeline(); //Should be called only once in the beggining
        //solr = SolrManager.getInstance("http://localhost:8080/solr/WebPages");
    }

    @PreDestroy
    public void cleanUp() throws Exception {
        System.out.println("Spring Container destroy");
        MorphiaManager.tearDown();
        if (crawlerCtrler != null)
            crawlerCtrler.shutdown();
    }

    ////////////////////////////////////////////////////////
    ///////// NAMED ENTITIES     ///////////////////////////
    ///////////////////////////////////////////////////////

    @RequestMapping(value = "/text/entities", method = RequestMethod.POST, produces = "application/json", consumes = "application/json")
    @ResponseBody
    public List<NamedEntity> entitiesFromString(@RequestBody Requests.EntitiesPostRequest req) throws Exception {
        TextPreprocessing textPre = new TextPreprocessing(req.text);
        // Get the cleaned text
        ArrayList<String> cleanedText = textPre.getCleanedSentences();
        //Run the NER
        List<NamedEntity> names = nte.tagIt(cleanedText);
        return names;
    }

    @RequestMapping(value = "/text/entities", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<NamedEntity> entitiesFromURL(
            @RequestParam(value = "url", required = true) String urlStr) throws Exception {

        long now = System.currentTimeMillis();
        BoilerpipeContentExtraction bp = new BoilerpipeContentExtraction();
        //Extract content from URL
        Content c = bp.contentFromURL(urlStr);
        System.out.println("Boilerpipe time " + (System.currentTimeMillis() - now));
        String text = c.getTitle() + " " + c.getText();
        now = System.currentTimeMillis();
        TextPreprocessing textPre = new TextPreprocessing(text);
        // Get the cleaned text
        ArrayList<String> cleanedText = textPre.getCleanedSentences();
        System.out.println("Preprocessing time " + (System.currentTimeMillis() - now));
        //Run the NER
        now = System.currentTimeMillis();
        List<NamedEntity> names = nte.tagIt(cleanedText);
        System.out.println("Named Entity time " + (System.currentTimeMillis() - now));
        return names;
    }

    ////////////////////////////////////////////////////////
    ///////// CRAWLER            ///////////////////////////
    ///////////////////////////////////////////////////////

    @RequestMapping(value = "/crawls/add", method = RequestMethod.POST, produces = "application/json", consumes = "application/json")
    @ResponseBody
    public CrawlJob submitCrawlingJob(@RequestBody Requests.CrawlPostRequest request) throws RevealException {
        try {
            return crawlerCtrler.submit(request.isNew, request.collection, request.keywords);
        } catch (Exception ex) {
            throw new RevealException(ex.getMessage(), ex);
        }
    }

    @RequestMapping(value = "/crawls/{id}/delete", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public boolean deleteCrawlingJob(@PathVariable(value = "id") String id) throws RevealException {
        try {
            crawlerCtrler.delete(id);
            return true;
        } catch (Exception ex) {
            throw new RevealException("Error when deleting", ex);
        }
    }

    @RequestMapping(value = "/crawls/{id}/stop", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public CrawlJob cancelCrawlingJob(@PathVariable(value = "id") String id) {
        return crawlerCtrler.cancel(id);
    }

    @RequestMapping(value = "/crawls/{id}/status", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public Responses.CrawlStatus getCrawlingJobStatus(@PathVariable(value = "id") String id) {
        return crawlerCtrler.getStatus(id);
    }

    /**
     * @return a list of CrawlRequests that are either RUNNING or WAITING
     */
    @RequestMapping(value = "/crawls/status", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<Responses.CrawlStatus> getCrawlerStatus() {
        List<Responses.CrawlStatus> s = crawlerCtrler.getActiveCrawls();
        return s;
    }

    ////////////////////////////////////////////////////////
    ///////// COLLECTIONS INDEXING             /////////////
    ///////////////////////////////////////////////////////

    /**
     * Adds a collection with the specified name
     * <p>
     * Example: http://localhost:8090/reveal/mmapi/collections/add?name=re, defaultValue = "-1"vealsample
     *
     * @param name
     * @return
     */
    @RequestMapping(value = "/collections/add", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public Responses.IndexResponse collectionsAdd(
            @RequestParam(value = "name", required = true) String name,
            @RequestParam(value = "size", required = false, defaultValue = "100000") int numVectors) {
        try {
            VisualIndexerFactory.getVisualIndexer(name);
            return new Responses.IndexResponse();
        } catch (Exception ex) {
            return new Responses.IndexResponse(false, ex.toString());
        }
    }

    ////////////////////////////////////////////////////////
    ///////// MEDIA NEW API WITH SIMMO FRAMEWORK ///////////
    ///////////////////////////////////////////////////////

    @RequestMapping(value = "/media/{collection}", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public Responses.MediaResponse mediaItemsV2(@RequestParam(value = "count", required = false, defaultValue = "10") int count,
                                                @RequestParam(value = "offset", required = false, defaultValue = "0") int offset,
                                                @RequestParam(value = "type", required = false) String type,
                                                @PathVariable(value = "collection") String collection) {
        Responses.MediaResponse response = new Responses.MediaResponse();
        if (type == null || type.equalsIgnoreCase("image")) {
            MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, collection);
            //response.images = imageDAO.getDatastore().find(Image.class).order("lastModifiedDate").offset(offset).limit(count).asList();
            response.images = imageDAO.getItems(count, offset);
            response.numImages = imageDAO.count();
        }
        if (type == null || type.equalsIgnoreCase("video")) {
            MediaDAO<Video> videoDAO = new MediaDAO<>(Video.class, collection);
            response.videos = videoDAO.getItems(count, offset);
            //response.images = videoDAO.getDatastore().find(Image.class).order("lastModifiedDate").offset(offset).limit(count).asList();
            response.numVideos = videoDAO.count();
        }
        response.offset = offset;
        return response;
    }

    @RequestMapping(value = "/media/{collection}/{id}", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public Media mediaItemByIdV2(@PathVariable(value = "collection") String collection,
                                 @PathVariable("id") String id) {
        Media result;
        MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, collection);
        result = imageDAO.get(new ObjectId(id).toString());
        if (result == null) {
            MediaDAO<Video> videoDAO = new MediaDAO<>(Video.class, collection);
            result = videoDAO.get(new ObjectId(id).toString());
        }
        return result;
    }

    @RequestMapping(value = "/media/{collection}/search", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public Responses.MediaResponse mediaItemsSearchV2(
            @PathVariable(value = "collection") String collection,
            @RequestParam(value = "date", required = false, defaultValue = "-1") long date,
            @RequestParam(value = "w", required = false, defaultValue = "0") int w,
            @RequestParam(value = "h", required = false, defaultValue = "0") int h,
            @RequestParam(value = "count", required = false, defaultValue = "10") int count,
            @RequestParam(value = "offset", required = false, defaultValue = "0") int offset,
            @RequestParam(value = "query", required = false) String query,
            @RequestParam(value = "type", required = false) String type) {


        Responses.MediaResponse response = new Responses.MediaResponse();
        if (type == null || type.equalsIgnoreCase("image")) {
            MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, collection);
            if (query != null) {
                Pattern p = Pattern.compile(query, Pattern.CASE_INSENSITIVE);
                Query<Image> q = imageDAO.createQuery();
                q.and(
                        q.criteria("lastModifiedDate").greaterThanOrEq(new Date(date)),
                        q.criteria("width").greaterThanOrEq(w),
                        q.criteria("height").greaterThanOrEq(h),
                        q.or(
                                q.criteria("title").equal(p),
                                q.criteria("description").equal(p)
                        )
                );
                response.images = q.offset(offset).limit(count).asList();
            } else
                response.images = imageDAO.search("lastModifiedDate", new Date(date), w, h, count, offset);
            response.numImages = imageDAO.count();
        }
        if (type == null || type.equalsIgnoreCase("video")) {
            MediaDAO<Video> videoDAO = new MediaDAO<>(Video.class, collection);
            if (query != null) {
                Pattern p = Pattern.compile(query);
                Query<Video> q = videoDAO.createQuery();
                q.and(
                        q.criteria("lastModifiedDate").greaterThanOrEq(new Date(date)),
                        q.criteria("width").greaterThanOrEq(w),
                        q.criteria("height").greaterThanOrEq(h),
                        q.or(
                                q.criteria("title").equal(p),
                                q.criteria("description").equal(p)
                        )
                );
                response.videos = q.offset(offset).limit(count).asList();
            } else
                response.videos = videoDAO.search("creationDate", new Date(date), w, h, count, offset);
            response.numVideos = videoDAO.count();
        }
        response.offset = offset;
        return response;
    }

    private List<Responses.SimilarityResponse> simList2;
    private String lastImageUrl2;
    private double lastThreshold2;
    private boolean isBusy2 = false;

    @RequestMapping(value = "/media/{collection}/similar", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<Responses.SimilarityResponse> findSimilarImagesV2(@PathVariable(value = "collection") String collectionName,
                                                                  @RequestParam(value = "imageurl", required = true) String imageurl,
                                                                  @RequestParam(value = "offset", required = false, defaultValue = "0") int offset,
                                                                  @RequestParam(value = "count", required = false, defaultValue = "50") int count,
                                                                  @RequestParam(value = "threshold", required = false, defaultValue = "0.6") double threshold) {
        try {
            if (isBusy2)
                return new ArrayList<>();
            if (!imageurl.equals(lastImageUrl2) || simList2 == null || (simList2 != null && offset + count > simList2.size()) || lastThreshold2 != threshold) {
                isBusy2 = true;
                MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, collectionName);
                MediaDAO<Video> videoDAO = new MediaDAO<>(Video.class, collectionName);
                lastThreshold2 = threshold;
                lastImageUrl2 = imageurl;
                List<JsonResultSet.JsonResult> temp = VisualIndexerFactory.getVisualIndexer(collectionName).findSimilar(imageurl, threshold);
                simList2 = new ArrayList<>(temp.size());
                for (JsonResultSet.JsonResult r : temp) {
                    System.out.println("r.getExternalId " + r.getId());
                    Media found = imageDAO.getDatastore().find(Image.class).field("_id").equal(r.getId()).get();
                    if (found != null)
                        simList2.add(new Responses.SimilarityResponse(found, r.getRank()));
                    found = videoDAO.getDatastore().find(Video.class).field("_id").equal(r.getId()).get();
                    if (found != null)
                        simList2.add(new Responses.SimilarityResponse(found, r.getRank()));
                }
            }
            isBusy2 = false;
            if (simList2.size() < count)
                return simList2;
            else
                return simList2.subList(offset, offset + count);

        } catch (Exception e) {
            isBusy2 = false;
            System.out.println(e);
            return new ArrayList<>();
        }
    }

    ////////////////////////////////////////////////////////
    ////////// C L U S T E R I N G /////////////////////////
    ///////////////////////////////////////////////////////

    @RequestMapping(value = "/media/{collection}/cluster", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public boolean clusterCommand(@PathVariable(value = "collection") String collection,
                                  @RequestParam(value = "eps", required = true, defaultValue = "1.0") double eps,
                                  @RequestParam(value = "minpoints", required = true, defaultValue = "3") int minpoints) {

        System.out.println("DBSCAN for " + collection + " eps= " + eps + " minpoints= " + minpoints);
        List<ClusterableMedia> list = new ArrayList<>();
        //images
        MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, collection);
        DAO<gr.iti.mklab.simmo.core.cluster.Cluster, String> clusterDAO = new BasicDAO<>(gr.iti.mklab.simmo.core.cluster.Cluster.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB(collection).getName());
        clusterDAO.deleteByQuery(clusterDAO.createQuery());
        List<Image> images = imageDAO.getItems((int) imageDAO.count(), 0);
        images.stream().forEach(i -> {
            Double[] vector = new Double[0];
            try {
                vector = VisualIndexerFactory.getVisualIndexer(collection).getVector(i.getId());
            } catch (ExecutionException e) {
                //ignore
            }
            if (vector != null && vector.length == 1024)
                list.add(new ClusterableMedia(i, ArrayUtils.toPrimitive(vector)));

        });
        //videos
        MediaDAO<Video> videoDAO = new MediaDAO<>(Video.class, collection);
        List<Video> videos = videoDAO.getItems((int) videoDAO.count(), 0);
        videos.stream().forEach(i -> {
            Double[] vector = new Double[0];
            try {
                vector = VisualIndexerFactory.getVisualIndexer(collection).getVector(i.getId());
            } catch (ExecutionException e) {
                //ignore
            }
            if (vector != null && vector.length == 1024)
                list.add(new ClusterableMedia(i, ArrayUtils.toPrimitive(vector)));

        });
        DBSCANClusterer<ClusterableMedia> clusterer = new DBSCANClusterer(eps, minpoints);
        List<Cluster<ClusterableMedia>> centroids = clusterer.cluster(list);
        System.out.println("DBSCAN NUMBER OF CLUSTERS " + centroids.size());
        for (Cluster<ClusterableMedia> c : centroids) {
            gr.iti.mklab.simmo.core.cluster.Cluster cluster = new gr.iti.mklab.simmo.core.cluster.Cluster();
            cluster.setSize(c.getPoints().size());
            c.getPoints().stream().forEach(clusterable -> cluster.addMember(clusterable.item));
            clusterDAO.save(cluster);
        }

        return true;
    }

    @RequestMapping(value = "/clusters/{collection}", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<gr.iti.mklab.simmo.core.cluster.Cluster> getClusters(@PathVariable(value = "collection") String collection,
                                                                @RequestParam(value = "offset", required = false, defaultValue = "0") int offset,
                                                                @RequestParam(value = "count", required = false, defaultValue = "50") int count) {
        DAO<gr.iti.mklab.simmo.core.cluster.Cluster, String> clusterDAO = new BasicDAO<>(gr.iti.mklab.simmo.core.cluster.Cluster.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB(collection).getName());
        return clusterDAO.getDatastore().find(gr.iti.mklab.simmo.core.cluster.Cluster.class).offset(offset).limit(count).asList();
    }

    private static class ClusterableMedia extends Media implements Clusterable {

        private double[] vector;
        private Media item;

        public ClusterableMedia(Media item, double[] vector) {
            this.item = item;
            this.vector = vector;
        }

        @Override
        public double[] getPoint() {
            return vector;
        }
    }


    ////////////////////////////////////////////////////////
    ///////// EXCEPTION HANDLING ///////////////////////////
    ///////////////////////////////////////////////////////

    @ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler(RevealException.class)
    @ResponseBody
    public RevealException handleCustomException(RevealException ex) {
        return ex;
    }

    public static void main(String[] args) throws Exception {
        Configuration.load("local.properties");
        MorphiaManager.setup("127.0.0.1");
        VisualIndexer.init();
        String collection = "testfinal";
        List<ClusterableMedia> list = new ArrayList<>();
        MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, collection);
        DAO<gr.iti.mklab.simmo.core.cluster.Cluster, String> clusterDAO = new BasicDAO<>(gr.iti.mklab.simmo.core.cluster.Cluster.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB(collection).getName());
        List<Image> images = imageDAO.getItems((int) imageDAO.count(), 0);
        images.stream().forEach(i -> {
            Double[] vector = new Double[0];
            try {
                System.out.println("Before getVector ");
                vector = VisualIndexerFactory.getVisualIndexer(collection).getVector(i.getId());
                System.out.println("vector length " + vector.length);
            } catch (ExecutionException e) {
                //ignore
            }
            if (vector != null && vector.length == 1024)
                list.add(new ClusterableMedia(i, ArrayUtils.toPrimitive(vector)));

        });
        DBSCANClusterer<ClusterableMedia> clusterer = new DBSCANClusterer(1.0, 3);
        List<Cluster<ClusterableMedia>> centroids = clusterer.cluster(list);
        System.out.println("DBSCAN NUMBER OF CLUSTERS " + centroids.size());
        for (Cluster<ClusterableMedia> c : centroids) {
            gr.iti.mklab.simmo.core.cluster.Cluster cluster = new gr.iti.mklab.simmo.core.cluster.Cluster();
            //mc.setCount(c.getPoints().size());
            c.getPoints().stream().forEach(clusterable -> cluster.addMember(clusterable.item));
            clusterDAO.save(cluster);
        }
        MorphiaManager.tearDown();
    }
}