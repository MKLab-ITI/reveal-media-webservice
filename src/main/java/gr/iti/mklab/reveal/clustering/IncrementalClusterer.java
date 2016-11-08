package gr.iti.mklab.reveal.clustering;

import com.aliasi.tokenizer.TokenizerFactory;

import gr.iti.mklab.reveal.util.Configuration;
import gr.iti.mklab.reveal.visual.VisualIndexClient;
import gr.iti.mklab.simmo.core.annotations.Clustered;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.items.Media;
import gr.iti.mklab.simmo.core.items.Video;
import gr.iti.mklab.simmo.core.morphia.MediaDAO;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.log4j.Logger;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;

import java.util.ArrayList;
import java.util.List;

/**
 * A clustering runnable which clusters the given number of images and videos from
 * the specified collection using the supplied configuration options.
 *
 * @author kandreadou
 */
public class IncrementalClusterer implements Runnable {

	private Logger _logger = Logger.getLogger(ClusteringCallable.class);
			
    private String collection;
    private int count;
    private double eps;
    private int minpoints;

    private boolean isRunning = true;

	private MediaDAO<Image> imageDAO;
	private MediaDAO<Video> videoDAO;
    
    public IncrementalClusterer(String collection, int count, double eps, int minpoints) {
        this.collection = collection;
        this.count = count;
        this.eps = eps;
        this.minpoints = minpoints;
        
        this.imageDAO = new MediaDAO<Image>(Image.class, collection);
        this.videoDAO = new MediaDAO<Video>(Video.class, collection);
    }

    @Override
    public void run() {
    	
    	_logger.info("Run DBSCAN for " + collection + ", eps=" + eps + ", minpoints=" + minpoints + ", count= " + count);
        
        String indexServiceHost = "http://" + Configuration.INDEX_SERVICE_HOST + ":8080/VisualIndexService";
		VisualIndexClient vIndexClient = new VisualIndexClient(indexServiceHost, collection);  
		
        TokenizerFactory tokFactory = new NormalizedTokenizerFactory();
        //First get the existing clusters for this collection
        DAO<gr.iti.mklab.simmo.core.cluster.Cluster, String> clusterDAO = new BasicDAO<>(gr.iti.mklab.simmo.core.cluster.Cluster.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB(collection).getName());
        
        
        while(isRunning) {
        	List<gr.iti.mklab.simmo.core.cluster.Cluster> savedClusters = clusterDAO.find().asList();
        	List<Cluster<VisualClusterable>> existingClusters = new ArrayList<Cluster<VisualClusterable>>();
        	savedClusters.stream().forEach(dbCluster -> {
                Cluster<VisualClusterable> cluster = new Cluster<VisualClusterable>();
                dbCluster.getMembers().stream().forEach(mediaItem -> {
                	String id = ((Media) mediaItem).getId();
    				Double[] vector = vIndexClient.getVector(id);
    				if(vector != null && vector.length == 1024) {
    					cluster.addPoint(new VisualClusterable((Media) mediaItem, ArrayUtils.toPrimitive(vector)));	
    				}	
                });
                existingClusters.add(cluster);
            });	

            List<VisualClusterable> mediaToBeClustered = new ArrayList<VisualClusterable>();
            
            List<Image> images = imageDAO.getIndexedNotClustered(count);
            images.stream().forEach(i -> {
                Double[] vector = vIndexClient.getVector(i.getId());
                if (vector != null && vector.length == 1024) {
                	mediaToBeClustered.add(new VisualClusterable(i, ArrayUtils.toPrimitive(vector)));
                }
            });
            
            List<Video> videos = videoDAO.getIndexedNotClustered(count);
            videos.stream().forEach(i -> {
                Double[] vector = vIndexClient.getVector(i.getId());
                if (vector != null && vector.length == 1024) {
                	mediaToBeClustered.add(new VisualClusterable(i, ArrayUtils.toPrimitive(vector)));
            	}
            });
        	
            _logger.info(mediaToBeClustered.size() + " new media to be clustered for " + collection);
            
            DBSCANClusterer<VisualClusterable> clusterer = new DBSCANClusterer<VisualClusterable>(eps, minpoints);
            List<Cluster<VisualClusterable>> centroids = clusterer.clusterIncremental(mediaToBeClustered, existingClusters);
            _logger.info("DBSCAN found " + centroids.size() + " clusters for " + collection);
            
            clusterDAO.deleteByQuery(clusterDAO.createQuery());
            for (Cluster<VisualClusterable> centroid : centroids) {
                //List<Media> mediaItems = new ArrayList<Media>();
                gr.iti.mklab.simmo.core.cluster.Cluster cluster = new gr.iti.mklab.simmo.core.cluster.Cluster();
                centroid.getPoints().stream().forEach(clusterable -> {
                    Clustered clusterAnnotation = new Clustered(cluster.getId());
                    //mediaItems.add(media);
                    //if (media instanceof Image) {
                    //    imageDAO.save((Image) media);
                    //} else {
                    //    videoDAO.save((Video) media);
                    //}
                });
                
                //_logger.info("Collection: " + collection + ". Initial size of media items: " + mediaItems.size());
                //List<Media> filteredNomralized = TextDeduplication.filterNormalizedDuplicates(mediaItems, tokFactory);
                //_logger.info("Collection: " + collection + ". After normalization size: " + filteredNomralized.size());
                
                //List<Media> filteredJaccard = TextDeduplication.filterMediaJaccard(filteredNomralized, tokFactory, 0.5);
                //_logger.info("Collection: " + collection + ". After jaccard size: " + filteredJaccard.size());
                
                //filteredJaccard.stream().forEach(m -> cluster.addMember(m));
                //cluster.setSize(filteredJaccard.size());
                if (cluster.getSize() < 100) {
                    clusterDAO.save(cluster);
                }
            }      
        }
    }

  
}
