package gr.iti.mklab.reveal.web;

import eu.socialsensor.framework.common.domain.WebPage;
import gr.iti.mklab.reveal.mongo.RevealMediaClusterDaoImpl;
import gr.iti.mklab.reveal.mongo.RevealMediaItemDaoImpl;
import gr.iti.mklab.reveal.solr.SolrManager;
import gr.iti.mklab.reveal.util.EntityForTweet;
import gr.iti.mklab.reveal.util.MediaCluster;
import gr.iti.mklab.reveal.util.MediaItem;
import gr.iti.mklab.reveal.util.NamedEntityDAO;
import gr.iti.mklab.reveal.visual.IndexingManager;
import gr.iti.mklab.visual.utilities.Answer;
import gr.iti.mklab.visual.utilities.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


@Controller
@RequestMapping("/mmapi")
public class RevealController {


    protected RevealMediaItemDaoImpl mediaDao;
    protected RevealMediaClusterDaoImpl clusterDAO;

    private static final Logger logger = LoggerFactory.getLogger(RevealController.class);

    protected SolrManager solr;

    //protected MongoManager mgr = new MongoManager("127.0.0.1", "Linear", "MediaItems");

    public RevealController() {
        String mongoHost = "127.0.0.1";

        try {
            mediaDao = new RevealMediaItemDaoImpl(mongoHost, "Showcase", "MediaItems");
            clusterDAO = new RevealMediaClusterDaoImpl(mongoHost, "Showcase", "MediaClusters");
            solr = SolrManager.getInstance("http://localhost:8080/solr/WebPages");
        } catch (Exception ex) {
            //ignore
        }
    }


    /**
     * Returns by default the last 10 media items or the number specified by count
     * <p/>
     * Example: http://localhost:8090/reveal/mmapi/media?count=20
     *
     * @param count
     * @param offset
     * @return
     */
    @RequestMapping(value = "/media", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<MediaItem> mediaItems(@RequestParam(value = "count", required = false, defaultValue = "10") int count,
                                      @RequestParam(value = "offset", required = false, defaultValue = "0") int offset,
                                      @RequestParam(value = "type", required = false) String type) {
        List<MediaItem> list = mediaDao.getMediaItems(offset, count, type);
        return list;
    }


    /**
     * Returns by default the last 10 media items or the number specified by count
     * <p/>
     * Example: http://localhost:8090/reveal/mmapi/media?count=20
     *
     * @param num
     * @return
     */
    @RequestMapping(value = "/mediaWithEntities", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<EntityResult> mediaItemsWithEntities(@RequestParam(value = "count", required = false, defaultValue = "10") int num) throws Exception {
        List<MediaItem> list = mediaDao.getMediaItems(num, 0, null);
        List<EntityResult> result = new ArrayList<EntityResult>(list.size());
        NamedEntityDAO dao = new NamedEntityDAO("160.40.51.20", "Showcase", "NamedEntities");
        for (MediaItem item : list) {
            EntityForTweet eft = dao.getItemForTweetId(item.getId());
            if (eft != null) {
                result.add(new EntityResult(item, eft.namedEntities));
            }
        }
        return result;
    }

    /**
     * Returns by default the last 10 media items or the number specified by count
     * <p/>
     * Example: http://localhost:8090/reveal/mmapi/media?count=20
     *
     * @return
     */
    @RequestMapping(value = "/media/clusters", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<MediaCluster> mediaClusters(@RequestParam(value = "count", required = false, defaultValue = "10") int count,
                                            @RequestParam(value = "offset", required = false, defaultValue = "0") int offset) {
        List<MediaCluster> clusters = clusterDAO.getSortedClusters(offset, count);
        for (MediaCluster c : clusters) {
            c.item = mediaDao.getItem(c.getMembers().iterator().next());
        }
        return clusters;
    }

    /**
     * Returns by default the last 10 media items or the number specified by count
     * <p/>
     * Example: http://localhost:8090/reveal/mmapi/media?count=20
     *
     * @param clusterId
     * @return
     */
    @RequestMapping(value = "/media/cluster/{id}", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<MediaItem> mediaCluster(@PathVariable(value = "id") String clusterId,
                                        @RequestParam(value = "count", required = false, defaultValue = "10") int count,
                                        @RequestParam(value = "offset", required = false, defaultValue = "0") int offset) {

        MediaCluster cluster = clusterDAO.getCluster(clusterId);
        int numMembers = cluster.getCount();
        if (offset > numMembers)
            return new ArrayList<>();
        if (offset + count > numMembers)
            count = numMembers - offset;
        int total = offset + count;
        String[] members = cluster.getMembers().toArray(new String[cluster.getCount()]);
        List<MediaItem> items = new ArrayList<>(count);
        for (int i = offset; i < total; i++) {
            items.add(mediaDao.getItem(members[i]));
        }
        return items;
    }

    /**
     * Returns the image with the specified id
     * <p/>
     * Example: http://localhost:8090/reveal/mmapi/media/image/6f1d874534e126dcf9296c9b050cef23
     *
     * @param mediaItemId
     * @return
     */
    @RequestMapping(value = "/media/{id}", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public MediaItem mediaItemById(@PathVariable("id") String mediaItemId) {
        MediaItem mi = mediaDao.getItem(mediaItemId);
        return mi;
    }

    /**
     * Searches for images with publicationTime, width and height GREATER than the provided values
     * Example: http://localhost:8090/reveal/mmapi/media/image/search?h=1000&w=2000
     *
     * @param date
     * @param w
     * @param h
     * @return
     */
    @RequestMapping(value = "/media/image/search", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<MediaItem> mediaItemsSearch(
            @RequestParam(value = "date", required = false, defaultValue = "-1") long date,
            @RequestParam(value = "w", required = false, defaultValue = "0") int w,
            @RequestParam(value = "h", required = false, defaultValue = "0") int h,
            @RequestParam(value = "query", required = false) String text,
            @RequestParam(value = "user", required = false) String username,
            @RequestParam(value = "type", required = false) String type,
            @RequestParam(value = "count", required = false, defaultValue = "10") int count,
            @RequestParam(value = "offset", required = false, defaultValue = "0") int offset) {

        List<MediaItem> list = mediaDao.search(username, text, w, h, date, count, offset, type);
        return list;
    }

    /**
     * Adds a collection with the specified name
     * <p/>
     * Example: http://localhost:8090/reveal/mmapi/collections/add?name=re, defaultValue = "-1"vealsample
     *
     * @param name
     * @return
     */
    @RequestMapping(value = "/collections/add", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public IndexingResult collectionsAdd(
            @RequestParam(value = "name", required = true) String name) {
        try {
            IndexingManager.getInstance().createIndex(name);
            return new IndexingResult();
        } catch (Exception ex) {
            return new IndexingResult(false, ex.toString());
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

    /*@RequestMapping(value = "/media/{collection}/index", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public String indexImageFromFile(@PathVariable("collection") String collectionName,
                             @RequestParam(value = "folder", required = false) String folder,
                             @RequestParam(value = "name", required = true) String filename) {
        try {
            return String.valueOf(IndexingManager.getInstance().indexImage("/home/kandreadou/Pictures/asdf/", filename, collectionName));
        } catch (Exception e) {
            return e.getMessage();
        }
    }*/

    /**
     * Indexes the image in the specified url
     * <p/>
     * http://localhost:8090/reveal/mmapi/media/revealsample_1024/index?imageurl=http%3A%2F%2Fww2.hdnux.com%2Fphotos%2F31%2F11%2F13%2F6591221%2F3%2F628x471.jpg
     *
     * @param collectionName
     * @param imageurl
     * @return
     */
    @RequestMapping(value = "/media/{collection}/index", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public String indexImageFromUrl(@PathVariable("collection") String collectionName,
                                    @RequestParam(value = "imageurl", required = true) String imageurl) {
        try {
            return String.valueOf(IndexingManager.getInstance().indexImage(imageurl, collectionName));
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    /**
     * Gets statistics for the given collection
     * <p/>
     * Example: http://localhost:8090/reveal/mmapi/collections/revealsample_1024/statistics
     *
     * @param collectionName
     * @return
     */
    @RequestMapping(value = "/collections/{collection}/statistics", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public String getStatistics(@PathVariable("collection") String collectionName) {
        try {
            return IndexingManager.getInstance().statistics(collectionName);
        } catch (Exception e) {
            return e.getMessage();
        }
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
    public IndexingResult indexWithPost(
            @RequestBody IndexingRequest request) {

        String msg = null;
        for (String url : request.getUrls()) {
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
            return new IndexingResult();
        else
            return new IndexingResult(false, msg);
    }

    private List<SimilarityResult> finallist;
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
                for (Result r : temp) {
                    if (r.getDistance() <= threshold) {
                        MediaItem found = mediaDao.getItem(r.getExternalId());
                        if (found.getPublicationTime() > 0)
                            finallist.add(new SimilarityResult(found, r.getDistance()));
                    }
                }
                Collections.sort(finallist, new Comparator<SimilarityResult>() {
                    @Override
                    public int compare(SimilarityResult result, SimilarityResult result2) {
                        return Long.compare(result.getItem().getPublicationTime(), result2.getItem().getPublicationTime());
                    }
                });
            }
            return finallist.subList(offset, offset + count);
        } catch (Exception e) {
            return new ArrayList<>();
        }
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

    @RequestMapping(value = "/media/webpages/search", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<WebPage> findItemsByKeyword(@RequestParam(value = "query", required = true) String query,
                                            @RequestParam(value = "count", required = false, defaultValue = "50") int num) {
        return solr.collectMediaItemsByQuery(query, num);

    }

    /*@RequestMapping(value = "/media/test", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<DBObject> mediaFromManager() {
        return mgr.search();
    }*/


    /////////////////////////////////////////////////////
    ///////////// TEST STUFF ///////////////////////////
    ////////////////////////////////////////////////////

    /*@ExceptionHandler(IndexingException.class)
    public ModelAndView handleIndexingException(HttpServletRequest request, Exception ex){
        logger.error("Requested URL="+request.getRequestURL());
        logger.error("Exception Raised="+ex);

        ModelAndView modelAndView = new ModelAndView();
        modelAndView.addObject("exception", ex);
        modelAndView.addObject("url", request.getRequestURL());
        modelAndView.addObject("message", ex.getMessage());

        modelAndView.setViewName("/WEB-INF/pages/error.html");
        return modelAndView;
    }*/

    @RequestMapping(method = RequestMethod.GET)
    public String printWelcome(ModelMap model) {
        model.addAttribute("message", "Hello REVEAL!");
        return "hello";
    }

    @RequestMapping(value = "/greeting", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public Greeting greeting(@RequestParam(value = "name", required = false, defaultValue = "World") String name) {
        return new Greeting(5, "test");
    }

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();

    public class Greeting {

        private final long id;
        private final String content;

        public Greeting(long id, String content) {
            this.id = id;
            this.content = content;
        }

        public long getId() {
            return id;
        }

        public String getContent() {
            return content;
        }
    }

    public static void main(String[] args) throws Exception {
        int offset = 5;
        int count = 5;
        int total = offset + count;
        Answer answer = IndexingManager.getInstance().findSimilar("https://pbs.twimg.com/media/BhZpUMmIIAAQOsr.png", "showcase", total);
        List<SimilarityResult> items = new ArrayList<SimilarityResult>();
        for (int i = offset; i < total; i++) {
            Result r = answer.getResults()[i];
            System.out.println(i);
            //items.add(new SimilarityResult(mediaDao.getItem(r.getExternalId()), r.getDistance()));
        }
    }
}