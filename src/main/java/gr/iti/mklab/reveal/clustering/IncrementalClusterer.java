package gr.iti.mklab.reveal.clustering;

import gr.iti.mklab.reveal.clustering.ensemble.TextDistance;
import gr.iti.mklab.reveal.clustering.ensemble.TextVectorFeature;
import gr.iti.mklab.reveal.clustering.ensemble.VectorCentroid;
import gr.iti.mklab.reveal.crawler.LinkDetectionRunner;
import gr.iti.mklab.reveal.summarization.Vector;
import gr.iti.mklab.reveal.summarization.Vocabulary;
import gr.iti.mklab.reveal.util.Configuration;
import gr.iti.mklab.reveal.visual.VisualIndexClient;
import gr.iti.mklab.simmo.core.annotations.Clustered;
import gr.iti.mklab.simmo.core.documents.Webpage;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.items.Media;
import gr.iti.mklab.simmo.core.items.Video;
import gr.iti.mklab.simmo.core.morphia.MediaDAO;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import gr.iti.mklab.simmo.core.morphia.ObjectDAO;

import org.apache.log4j.Logger;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.UpdateOperations;

import com.oculusinfo.ml.DataSet;
import com.oculusinfo.ml.Instance;
import com.oculusinfo.ml.feature.numeric.NumericVectorFeature;
import com.oculusinfo.ml.feature.numeric.centroid.MeanNumericVectorCentroid;
import com.oculusinfo.ml.feature.numeric.distance.EuclideanDistance;
import com.oculusinfo.ml.unsupervised.cluster.Cluster;
import com.oculusinfo.ml.unsupervised.cluster.ClusterResult;
import com.oculusinfo.ml.unsupervised.cluster.threshold.ThresholdClusterer;

import edu.stanford.nlp.util.ArrayUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 
 */
public class IncrementalClusterer implements Runnable {

	private Logger LOGGER = Logger.getLogger(IncrementalClusterer.class);
			
    private String collection;

    private boolean isRunning = true;
    
    private int count = 100;
	private long SLEEP_TIME = 60l * 1000l;
	
	private MediaDAO<Image> imageDAO;
	private MediaDAO<Video> videoDAO;
	private ObjectDAO<Webpage> pageDAO;
	
	private DAO<gr.iti.mklab.simmo.core.cluster.Cluster, String> clusterDAO;
	private VisualIndexClient vIndexClient;

	private double threshold;
	private double textualWeight;
	private double visualWeight;
	
    public IncrementalClusterer(String collection, double threshold, double textualWeight, double visualWeight) {
        this.collection = collection;

        this.threshold = threshold;
        this.textualWeight = textualWeight;
        this.visualWeight = visualWeight;
        
        this.imageDAO = new MediaDAO<Image>(Image.class, collection);
        this.videoDAO = new MediaDAO<Video>(Video.class, collection);
        this.pageDAO = new ObjectDAO<Webpage>(Webpage.class, collection);
    
        String indexServiceHost = "http://" + Configuration.INDEX_SERVICE_HOST + ":8080/VisualIndexService";
		vIndexClient = new VisualIndexClient(indexServiceHost, collection);  
		
        clusterDAO = new BasicDAO<gr.iti.mklab.simmo.core.cluster.Cluster, String>(gr.iti.mklab.simmo.core.cluster.Cluster.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), collection);
    
    }

    public void stop() {
    	isRunning = false;
    }
    
    @Override
    public void run() {
	
    	Map<String, Cluster> clustersMap = new HashMap<String, Cluster>();
        Set<String> processed = new HashSet<String>();	
        while(isRunning) {
    
        	Map<String, Media> mediaToBeClustered = new HashMap<String, Media>();
        	Map<String, String> texts = new HashMap<String, String>();
            Map<String, Double[]> visualVectors = new HashMap<String, Double[]>();
            
            List<Image> images = imageDAO.getIndexedNotClustered(count);
            images.stream().forEach(m -> {
            	if(!processed.contains(m.getId())) {
            		processed.add(m.getId());
            		mediaToBeClustered.put(m.getId(), m);
                	Double[] vector = vIndexClient.getVector(m.getId());
                	if (vector != null && vector.length == 1024) {
                		visualVectors.put(m.getId(), vector);
                	}
            		texts.put(m.getId(), m.getTitle());
            	}
            });
            
            List<Video> videos = videoDAO.getIndexedNotClustered(count);
            videos.stream().forEach(m -> {
            	if(!processed.contains(m.getId())) {
            		processed.add(m.getId());
            		mediaToBeClustered.put(m.getId(), m);
            		Double[] vector = vIndexClient.getVector(m.getId());
                	if (vector != null && vector.length == 1024) {
                		visualVectors.put(m.getId(), vector);
                	}
                	texts.put(m.getId(), m.getTitle());
            	}
            });
		
            Map<String, Vector> textualVectors = Vocabulary.createVocabulary(texts, 2);
            if(mediaToBeClustered.isEmpty()) {
            	try {
					Thread.sleep(SLEEP_TIME);
				} catch (InterruptedException e) {
					LOGGER.error(e.getMessage());
				}
            	
            	if(!isRunning)
					break;
            	else
            		continue;
            }
            
            
            
            LOGGER.info(collection + " clustering: " + mediaToBeClustered.size() + " media items to be clustered. " + visualVectors.size() + " visual vectors. " + textualVectors.size());
            
            Set<String> discardedMedia = new HashSet<String>();
            DataSet ds = new DataSet();
            for(String mId : mediaToBeClustered.keySet()) {
            	
            	Instance instance = new Instance();
            	instance.setId(mId);
            	
            	Vector vector = textualVectors.get(mId);
            	if(vector != null && vector.getLength() > 0) {
            		TextVectorFeature tfv = new TextVectorFeature("text");
            		tfv.setValue(vector);
					instance.addFeature(tfv);
            	}
            	
				Double[] visualVector = visualVectors.get(mId);
				if(visualVector != null) {
					NumericVectorFeature vfv = new NumericVectorFeature("visual");
					vfv.setValue(ArrayUtils.toPrimitive(visualVector));
					instance.addFeature(vfv);
				}
				
				if(instance.numFeatures() > 0) {
					ds.add(instance);
				}
				else {
					discardedMedia.add(mId);
				}
			}

            ThresholdClusterer clusterer = new ThresholdClusterer();
        	clusterer.setThreshold(threshold);
    		clusterer.registerFeatureType("text", VectorCentroid.class, new TextDistance(textualWeight));
    		clusterer.registerFeatureType("visual", MeanNumericVectorCentroid.class, new EuclideanDistance(visualWeight));
    		
    		int newClusters = 0;
            ClusterResult clusterResult = clusterer.doIncrementalCluster(ds, new ArrayList<Cluster>(clustersMap.values()));
            for(Cluster cluster : clusterResult) {
            	if(clustersMap.containsKey(cluster.getId())) {
            		List<Media> newMembers = new ArrayList<Media>();
            		for(Instance instance : cluster.getMembers()) {
            			String mId = instance.getId();
            			if(mediaToBeClustered.containsKey(mId)) {
            				newMembers.add(mediaToBeClustered.get(mId));
            			}
            		}
            		
            		if(newMembers.size() > 0) {
            			updateCluster(cluster, newMembers);
            		}
            	}
            	else {
            		newClusters++;
            		clustersMap.put(cluster.getId(), cluster);
            		saveCluster(cluster, mediaToBeClustered);
            	}
            	
            }
            
            LOGGER.info(collection + " clustering: " + clusterResult.size() + " clusters found. " + newClusters + " new clusters, "+ clustersMap.size() + " in total.");
            
            LOGGER.info("Delete " + discardedMedia.size() + " media, discarded from clustering of " + collection);
            for(String mId : discardedMedia) {
            	Media media = mediaToBeClustered.get(mId);
            	deleteMedia(media);	
            }
        }
        
    }
    
    public void saveCluster(Cluster cluster, Map<String, Media> mediaToBeClustered) {
   		gr.iti.mklab.simmo.core.cluster.Cluster simmoCluster = new gr.iti.mklab.simmo.core.cluster.Cluster();
		simmoCluster.setId(cluster.getId());
		for(Instance instance : cluster.getMembers()) {
			String mId = instance.getId();
			Media media = mediaToBeClustered.get(mId);
			if(media != null) {
				simmoCluster.addMember(media);
				Clustered annotation = new Clustered(cluster.getId());
				if (media instanceof Image) {
					Query<Image> q = imageDAO.createQuery().filter("_id", mId);
					UpdateOperations<Image> ops = imageDAO.createUpdateOperations().add("annotations", annotation, false);
					imageDAO.update(q, ops);
				} else {
					Query<Video> q = videoDAO.createQuery().filter("_id", mId);
					UpdateOperations<Video> ops = videoDAO.createUpdateOperations().add("annotations", annotation, false);
					videoDAO.update(q, ops);
				}
			}
		}
		simmoCluster.setSize(cluster.size());
		clusterDAO.save(simmoCluster);
		
		
    }
    
    public void updateCluster(Cluster cluster, List<Media> newMembers) {
   	
		Query<gr.iti.mklab.simmo.core.cluster.Cluster> query = clusterDAO.createQuery().filter("_id", cluster.getId());
		
		UpdateOperations<gr.iti.mklab.simmo.core.cluster.Cluster> ops = clusterDAO.createUpdateOperations();
		
		ops.addAll("members", newMembers, false);
		ops.inc("size", newMembers.size());
		
		clusterDAO.update(query, ops);
		
		for(Media media : newMembers) {
			Clustered annotation = new Clustered(cluster.getId());
			if (media instanceof Image) {
				Query<Image> q = imageDAO.createQuery().filter("_id", media.getId());
				imageDAO.update(q, imageDAO.createUpdateOperations().add("annotations", annotation, false));
			} else {
				Query<Video> q = videoDAO.createQuery().filter("_id", media.getId());
				videoDAO.update(q, videoDAO.createUpdateOperations().add("annotations", annotation, false));
			}
		}
		
    }
    
    private void deleteMedia(Media media) {
    	if(media == null)
    		return;
    	
    	if(media instanceof Image) {
        	imageDAO.delete((Image)media);
        	pageDAO.deleteById(media.getId());
        	if (LinkDetectionRunner.LAST_POSITION > 0) {
        		LinkDetectionRunner.LAST_POSITION--;
        	}
		}
        else if(media instanceof Video) {
			videoDAO.delete((Video)media);
		}
    	else {
    		LOGGER.info("Unknown instance for " + media.getId());
    	}
    }
    
    public static void main(String...args) {
        MorphiaManager.setup("160.40.50.207");
        
    	IncrementalClusterer cl = new IncrementalClusterer("test", 0.5, 1., 0.5);
    	cl.run();
    }
   
}
