package gr.iti.mklab.reveal.web;

import gr.iti.mklab.framework.client.search.visual.JsonResultSet;
import gr.iti.mklab.reveal.configuration.Configuration;
import gr.iti.mklab.reveal.crawler.CrawlQueueController;
import gr.iti.mklab.reveal.crawler.CrawlRequest;
import gr.iti.mklab.reveal.text.NameThatEntity;
import gr.iti.mklab.reveal.text.TextPreprocessing;
import gr.iti.mklab.reveal.text.htmlsegmentation.BoilerpipeContentExtraction;
import gr.iti.mklab.reveal.text.htmlsegmentation.Content;

import gr.iti.mklab.reveal.visual.IndexingManager;
import gr.iti.mklab.reveal.visual.VisualIndexer;
import gr.iti.mklab.simmo.annotations.NamedEntity;
import gr.iti.mklab.simmo.items.Image;
import gr.iti.mklab.simmo.items.Media;
import gr.iti.mklab.simmo.items.Video;
import gr.iti.mklab.simmo.morphia.MediaDAO;
import gr.iti.mklab.simmo.morphia.MorphiaManager;
import gr.iti.mklab.visual.utilities.Result;
import org.bson.types.ObjectId;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PreDestroy;
import java.util.*;
import java.util.regex.Pattern;


@Controller
@RequestMapping("/mmapi")
public class RevealController {


    private static final Logger logger = LoggerFactory.getLogger(RevealController.class);


    protected CrawlQueueController crawlerCtrler;

    protected NameThatEntity nte;

    //protected MongoManager mgr = new MongoManager("127.0.0.1", "Linear", "MediaItems");

    public RevealController() throws Exception {
        Configuration.load(getClass().getResourceAsStream("/local.properties"));
        String mongoHost = "127.0.0.1";
        MorphiaManager.setup(mongoHost);
        crawlerCtrler = new CrawlQueueController();
        //nte = new NameThatEntity();
        //nte.initPipeline(); //Should be called only once in the beggining
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

    @RequestMapping(value = "/crawls/{id}", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<Image> getCrawledImages(@PathVariable(value = "id") String id,
                                        @RequestParam(value = "count", required = false, defaultValue = "10") int count,
                                        @RequestParam(value = "offset", required = false, defaultValue = "0") int offset) {
        return crawlerCtrler.getImages(id, count, offset);
    }

    @RequestMapping(value = "/crawls/add", method = RequestMethod.POST, produces = "application/json", consumes = "application/json")
    @ResponseBody
    public CrawlRequest submitCrawlingJob(@RequestBody Requests.CrawlPostRequest request) throws RevealException {
        try {
            String rootCrawlerDir = Configuration.CRAWLS_FOLDER;
            return crawlerCtrler.submit(request.isNew, rootCrawlerDir + "crawl_" + request.collectionName, request.collectionName, request.keywords);
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
    public CrawlRequest cancelCrawlingJob(@PathVariable(value = "id") String id) {
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
            IndexingManager.getInstance().createIndex(name, numVectors);
            return new Responses.IndexResponse();
        } catch (Exception ex) {
            return new Responses.IndexResponse(false, ex.toString());
        }
    }

    @RequestMapping(value = "/media/image/index", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public String indexImageFromFile(
            @RequestParam(value = "folder", required = false) String folder,
            @RequestParam(value = "name", required = true) String filename) {
        try {
            return String.valueOf(IndexingManager.getInstance().indexImage("/home/kandreadou/Pictures/asdf/", filename, null));
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    /**
     * Indexes the image in the specified url
     * <p>
     * http://localhost:8090/reveal/mmapi/media/revealsample_1024/index?imageurl=http%3A%2F%2Fww2.hdnux.com%2Fphotos%2F31%2F11%2F13%2F6591221%2F3%2F628x471.jpg
     *
     * @param collectionName
     * @param imageurl
     * @return
     */
    @RequestMapping(value = "/media/{collection}/index", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public String indexImageFromUrl(@PathVariable("collection") String collectionName,
                                    @RequestParam(value = "imageurl", required = true) String imageurl) throws RevealException {
        try {
            if (IndexingManager.getInstance().indexImage(imageurl, collectionName))
                return imageurl + " has been indexed";
            else
                throw new RevealException("Error. Image " + imageurl + " has already been indexed", null);
        } catch (Exception e) {
            throw new RevealException(e.getMessage(), e);
        }
    }

    /**
     * Gets statistics for the given collection
     * <p>
     * Example: http://localhost:8090/reveal/mmapi/collections/revealsample_1024/statistics
     *
     * @param collectionName
     * @return
     */
    @RequestMapping(value = "/collections/{collection}/statistics", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public Responses.StatsResponse[] getStatistics(@PathVariable("collection") String collectionName) throws RevealException {
        return IndexingManager.getInstance().statistics(collectionName);
    }


    /**
     * Sends a post request
     * Example: http://localhost:8090/reveal/mmapi/media/image/index
     * Content-type: application/json
     * Content-body: {"collection":"WTFCollection","urls":["http://static4.businessinsider.com/image/5326130f69bedd780c549606-1200-924/putin-68.jpg","http://www.trbimg.com/img-531a4ce6/turbine/topic-peplt007593"]}
     *
     * @param request
     * @return
     */
    @RequestMapping(value = "/media/image/index", method = RequestMethod.POST, produces = "application/json", consumes = "application/json")
    @ResponseBody
    public Responses.IndexResponse indexWithPost(
            @RequestBody Requests.IndexPostRequest request) {

        String msg = null;
        for (String url : request.urls) {
            try {
                logger.debug("Indexing image " + url);
                IndexingManager.getInstance().indexImage(url, request.collection);
            } catch (Exception e) {
                logger.error(e.getMessage());
                msg += "Error indexing " + url + " " + e.getMessage();
            }
            logger.error(url);
        }
        if (msg == null)
            return new Responses.IndexResponse();
        else
            return new Responses.IndexResponse(false, msg);
    }

    ////////////////////////////////////////////////////////
    ///////// MEDIA NEW API WITH SIMMO FRAMEWORK ///////////
    ///////////////////////////////////////////////////////

    @RequestMapping(value = "/media/v2/{collection}", method = RequestMethod.GET, produces = "application/json")
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

    @RequestMapping(value = "/media/v2/{collection}/{id}", method = RequestMethod.GET, produces = "application/json")
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

    @RequestMapping(value = "/media/v2/{collection}/search", method = RequestMethod.GET, produces = "application/json")
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

    @RequestMapping(value = "/media/v2/{collection}/similar", method = RequestMethod.GET, produces = "application/json")
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
                int roundedToTheNextHundred = ((offset + count + 99) / 100) * 100;
                lastThreshold2 = threshold;
                lastImageUrl2 = imageurl;
                List<JsonResultSet.JsonResult>  temp = VisualIndexer.getInstance().findSimilar(imageurl, collectionName, roundedToTheNextHundred).getResults();
                System.out.println("results size " + temp.length);
                simList2 = new ArrayList<>(temp.length);
                for (Result r : temp) {
                    if (r.getDistance() <= threshold) {
                        System.out.println("r.getExternalId " + r.getExternalId());
                        Image found = imageDAO.getDatastore().find(Image.class).field("_id").equal(new ObjectId(r.getExternalId())).get();
                        simList2.add(new Responses.SimilarityResponse(found, r.getDistance()));
                    }
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
    ///////// EXCEPTION HANDLING ///////////////////////////
    ///////////////////////////////////////////////////////

    @ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler(RevealException.class)
    @ResponseBody
    public RevealException handleCustomException(RevealException ex) {
        return ex;
    }

    public static void main(String[] args) throws Exception {
        /*int offset = 5;
        int count = 5;
        int total = offset + count;
        Answer answer = IndexingManager.getInstance().findSimilar("https://pbs.twimg.com/media/BhZpUMmIIAAQOsr.png", "showcase", total);
        List<SimilarityResult> items = new ArrayList<SimilarityResult>();
        for (int i = offset; i < total; i++) {
            Result r = answer.getResults()[i];
            System.out.println(i);
            //items.add(new SimilarityResult(mediaDao.getItem(r.getExternalId()), r.getDistance()));
        }*/
        Responses.MediaResponse response = new Responses.MediaResponse();
        MorphiaManager.setup("160.40.51.20");
        Pattern p = Pattern.compile("Foto");
        MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, "ebola");

        Query<Image> q = imageDAO.createQuery();
        q.and(
                q.criteria("lastModifiedDate").greaterThanOrEq(new Date(0)),
                q.criteria("width").greaterThanOrEq(200),
                q.criteria("height").greaterThanOrEq(200),
                q.or(
                        q.criteria("title").equal(p),
                        q.criteria("description").equal(p)
                )
        );
        response.images = q.offset(0).limit(50).asList();
        //response.images = imageDAO.getDatastore().find(Image.class).filter("title", p).filter("lastModifiedDate" + " >", new Date(0)).filter("width" + " >", 200).
        //       filter("height" + " >", 200).offset(0).limit(50).asList();
        MorphiaManager.tearDown();
        //String[] command = {"/bin/bash", "crawl9995.sh"};
        //ProcessBuilder p = new ProcessBuilder(command);
        // Process pr = p.start();
    }

    /*private List<SimilarityResult> finallist;
    private String lastImageUrl;
    private double lastThreshold;

    @RequestMapping(value = "/media/image/similar", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<SimilarityResult> findSimilarImages(@RequestParam(value = "collection", required = false) String collectionName,
                                                    @RequestParam(value = "imageurl", required = true) String imageurl,
                                                    @RequestParam(value = "offset", required = false, defaultValue = "0") int offset,
                                                    @RequestParam(value = "count", required = false, defaultValue = "50") int count,
                                                    @RequestParam(value = "threshold", required = false, defaultValue = "0.6") double threshold) {
        try {

            if (!imageurl.equals(lastImageUrl) || finallist == null || (finallist != null && offset + count > finallist.size()) || lastThreshold != threshold) {
                int total = offset + count;
                if (total < 100)
                    total = 100;
                lastThreshold = threshold;
                lastImageUrl = imageurl;
                Result[] temp = IndexingManager.getInstance().findSimilar(imageurl, collectionName, total).getResults();
                finallist = new ArrayList<>(temp.length);
                List<SimilarityResult> chronological = new ArrayList<>(temp.length);
                for (Result r : temp) {
                    if (r.getDistance() < threshold)
                        finallist.add(new SimilarityResult(mediaDao.getItem(r.getExternalId()), r.getDistance()));
                    else
                        chronological.add(new SimilarityResult(mediaDao.getItem(r.getExternalId()), r.getDistance()));
                }
                Collections.sort(chronological, new Comparator<SimilarityResult>() {
                    @Override
                    public int compare(SimilarityResult result, SimilarityResult result2) {
                        return Long.compare(result2.getItem().getPublicationTime(), result.getItem().getPublicationTime());
                    }
                });
                finallist.addAll(chronological);
            }
            return finallist.subList(offset, offset + count);
        } catch (Exception e) {
            return null;
        }
    }*/
}