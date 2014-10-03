package gr.iti.mklab.reveal.web;

import eu.socialsensor.framework.client.dao.MediaClusterDAO;
import eu.socialsensor.framework.client.dao.impl.MediaClusterDAOImpl;
import eu.socialsensor.framework.common.domain.MediaCluster;
import eu.socialsensor.framework.common.domain.MediaItem;
import eu.socialsensor.framework.common.domain.WebPage;
import gr.iti.mklab.reveal.mongo.RevealMediaItemDaoImpl;
import gr.iti.mklab.reveal.solr.SolrManager;
import gr.iti.mklab.reveal.util.EntityForTweet;
import gr.iti.mklab.reveal.util.NamedEntityDAO;
import gr.iti.mklab.reveal.visual.IndexingManager;
import gr.iti.mklab.visual.utilities.Answer;
import gr.iti.mklab.visual.utilities.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.*;

import javax.swing.text.html.parser.Entity;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Controller
@RequestMapping("/mmapi")
public class RevealController {


    protected RevealMediaItemDaoImpl mediaDao;
    protected MediaClusterDAOImpl clusterDAO;

    private static final Logger logger = LoggerFactory.getLogger(RevealController.class);

    protected SolrManager solr;

    //protected MongoManager mgr = new MongoManager("127.0.0.1", "Linear", "MediaItems");

    public RevealController() {
        String mongoHost = "127.0.0.1";

        try {
            mediaDao = new RevealMediaItemDaoImpl(mongoHost, "Showcase", "MediaItems");
            clusterDAO = new MediaClusterDAOImpl(mongoHost, "Showcase", "MediaClusters");
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
     * @param num
     * @return
     */
    @RequestMapping(value = "/media", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<MediaItem> mediaItems(@RequestParam(value = "count", required = false, defaultValue = "10") int num) {
        List<MediaItem> list = mediaDao.getLastMediaItems(num);
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
        List<MediaItem> list = mediaDao.getLastMediaItems(15);
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
     * @param clusterId
     * @return
     */
    @RequestMapping(value = "/media/cluster/{id}", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public MediaCluster mediaClusters(@PathVariable(value = "id") String clusterId) {
        return clusterDAO.getMediaCluster(clusterId);
    }

    /**
     * Returns the image with the specified id
     * <p/>
     * Example: http://localhost:8090/reveal/mmapi/media/image/6f1d874534e126dcf9296c9b050cef23
     *
     * @param mediaItemId
     * @return
     */
    @RequestMapping(value = "/media/image/{id}", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public MediaItem mediaItemById(@PathVariable("id") String mediaItemId) {
        MediaItem mi = mediaDao.getMediaItem(mediaItemId);
        return mi;
    }

    /**
     * Searches for images with publicationTime, width and height GREATER than the provided values
     * Example: http://localhost:8090/reveal/mmapi/media/image/search?h=1000&w=2000
     *
     * @param date
     * @param w
     * @param h
     * @param indexed
     * @return
     */
    @RequestMapping(value = "/media/image/search", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<MediaItem> mediaItemsSearch(
            @RequestParam(value = "date", required = false) Long date,
            @RequestParam(value = "w", required = false) Long w,
            @RequestParam(value = "h", required = false) Long h,
            @RequestParam(value = "indexed", required = false) Boolean indexed) {

        if (date == null) {
            date = new Date(0).getTime();
        }
        if (w == null) {
            w = Long.valueOf(0);
        }
        if (h == null) {
            h = Long.valueOf(0);
        }
        if (indexed == null) {
            indexed = false;
        }
        List<MediaItem> list = mediaDao.search(date, w, h, indexed);
        return list;
    }

    /**
     * Adds a collection with the specified name
     * <p/>
     * Example: http://localhost:8090/reveal/mmapi/collections/add?name=revealsample
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


    @RequestMapping(value = "/media/image/similar", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<SimilarityResult> findSimilarImages(@RequestParam(value = "collection", required = false) String collectionName,
                                                    @RequestParam(value = "imageurl", required = true) String imageurl) {
        try {
            Answer answer = IndexingManager.getInstance().findSimilar(imageurl, collectionName, 10);
            List<SimilarityResult> items = new ArrayList<SimilarityResult>();
            for (Result r : answer.getResults()) {
                items.add(new SimilarityResult(mediaDao.getMediaItem(r.getExternalId()), r.getDistance()));
            }
            return items;
        } catch (Exception e) {
            return null;
        }
    }

    @RequestMapping(value = "/media/search/text", method = RequestMethod.GET, produces = "application/json")
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
}