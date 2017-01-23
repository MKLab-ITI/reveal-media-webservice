package gr.iti.mklab.reveal.clustering;

import gr.iti.mklab.reveal.clustering.ensemble.TextDistance;
import gr.iti.mklab.reveal.clustering.ensemble.TextVectorFeature;
import gr.iti.mklab.reveal.clustering.ensemble.VectorCentroid;
import gr.iti.mklab.reveal.clustering.ensemble.VisualDistance;
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

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.UpdateOperations;

import com.oculusinfo.ml.DataSet;
import com.oculusinfo.ml.Instance;
import com.oculusinfo.ml.distance.DistanceFunction;
import com.oculusinfo.ml.feature.Feature;
import com.oculusinfo.ml.feature.numeric.NumericVectorFeature;
import com.oculusinfo.ml.feature.numeric.centroid.MeanNumericVectorCentroid;
import com.oculusinfo.ml.unsupervised.cluster.AbstractClusterer;
import com.oculusinfo.ml.unsupervised.cluster.Cluster;
import com.oculusinfo.ml.unsupervised.cluster.ClusterResult;
import com.oculusinfo.ml.unsupervised.cluster.Clusterer;
import com.oculusinfo.ml.unsupervised.cluster.dpmeans.DPMeans;
import com.oculusinfo.ml.unsupervised.cluster.threshold.ThresholdClusterer;

import edu.stanford.nlp.util.ArrayUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
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
    
    private int STEP = 100;
	private long SLEEP_TIME = 6l * 1000l;
	
	private MediaDAO<Image> imageDAO;
	private MediaDAO<Video> videoDAO;
	private ObjectDAO<Webpage> pageDAO;
	
	private DAO<gr.iti.mklab.simmo.core.cluster.Cluster, String> clusterDAO;
	private VisualIndexClient vIndexClient;

	private double threshold;
	private double textualWeight;
	private double visualWeight;

	private CLUSTERER_TYPE type;
	
	public static enum CLUSTERER_TYPE {
		THRESHOLD, 
		DPMEANS
	};
	
    public IncrementalClusterer(String collection, double threshold, double textualWeight, double visualWeight, CLUSTERER_TYPE type) {
        this.collection = collection;

        LOGGER.info("Initialize clusterer. Threshold: " + threshold + ", TextualWeight: " + textualWeight  + ", VisualWeight: " + visualWeight + ", Type: " + type);
        
        this.threshold = threshold;
        this.textualWeight = textualWeight;
        this.visualWeight = visualWeight;
        
        this.type = type;
        
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
	
    	try {
			boolean created = vIndexClient.createCollection();
			LOGGER.info("Collection " + collection + " created: " + created);
		} catch (IOException e) {
		}
    	
    	Map<String, Cluster> clustersMap = new HashMap<String, Cluster>();
        Set<String> processed = new HashSet<String>();	
        while(isRunning) {
    
        	LOGGER.info("Run clustering for " + collection + ": " + new Date());
        	
        	Map<String, Media> mediaToBeClustered = new HashMap<String, Media>();
        	Map<String, String> texts = new HashMap<String, String>();
            Map<String, Double[]> visualVectors = new HashMap<String, Double[]>();
            
            List<Image> images = imageDAO.getIndexedNotClustered(STEP);
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
            
            List<Video> videos = videoDAO.getIndexedNotClustered(STEP);
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
            
            int textualMissing = 0, visualMissing = 0;
            
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
            	else {
            		textualMissing++;
            	}
            	
				Double[] visualVector = visualVectors.get(mId);
				if(visualVector != null) {
					NumericVectorFeature vfv = new NumericVectorFeature("visual");
					vfv.setValue(ArrayUtils.toPrimitive(visualVector));
					instance.addFeature(vfv);
				}
				else {
					visualMissing++;
				}
				
				if(instance.numFeatures() > 0) {
					ds.add(instance);
				}
				else {
					discardedMedia.add(mId);
				}
			}
            
            LOGGER.info(collection + " clustering: " + textualMissing + " textual features are missing. " + visualMissing + " visual features are missing.");
            
            AbstractClusterer clusterer;
			if(type.equals(CLUSTERER_TYPE.THRESHOLD)) {
				ThresholdClusterer tClusterer = new ThresholdClusterer();
				tClusterer.setThreshold(threshold);
				
				clusterer = tClusterer;
            }
            else if(type.equals(CLUSTERER_TYPE.DPMEANS)) {
            	DPMeans dpmClusterer = new DPMeans(50, true);
            	dpmClusterer.setThreshold(threshold);
            	
            	clusterer = dpmClusterer;
            }
            else {
            	LOGGER.error("Cannot instantiate clusterer for " + collection + ". Type unknown: " + type);
            	isRunning = false;
            	break;
            }
			
    		clusterer.registerFeatureType("text", VectorCentroid.class, new TextDistance(textualWeight));
    		clusterer.registerFeatureType("visual", MeanNumericVectorCentroid.class, new VisualDistance(visualWeight));
    		
    		int newClusters = 0;
            ClusterResult clusterResult = clusterer.doIncrementalCluster(ds, new ArrayList<Cluster>(clustersMap.values()));
            for(Cluster cluster : clusterResult) {
            	
            	Map<String, Double> avgDistances = getAvgDistances(clusterer, cluster);
            	
            	String centroidId = getClusterCentroid(cluster, clusterer);
            	
            	if(clustersMap.containsKey(cluster.getId())) {
            		List<Media> newMembers = new ArrayList<Media>();
            		for(Instance instance : cluster.getMembers()) {
            			String mId = instance.getId();
            			if(mediaToBeClustered.containsKey(mId)) {
            				newMembers.add(mediaToBeClustered.get(mId));
            			}
            		}
            		
            		if(newMembers.size() > 0) {
            			LOGGER.info(collection + " clustering: update cluster " + cluster.getId() + " (" + cluster.size() + ") with " + newMembers.size() + " new members");
            			updateCluster(cluster, newMembers, centroidId, avgDistances);
            		}
            	}
            	else {
            		newClusters++;
            		LOGGER.info(collection + " clustering: save new cluster " + cluster.getId() + " (" + cluster.size() + ")");
            		saveCluster(cluster, mediaToBeClustered, centroidId,  avgDistances);
            	}
            	
            	clustersMap.put(cluster.getId(), cluster);
            }
            
            LOGGER.info(collection + " clustering: " + clusterResult.size() + " clusters found. " + newClusters + " new clusters, "+ clustersMap.size() + " in total.");
            
            LOGGER.info("Delete " + discardedMedia.size() + " media, discarded from clustering of " + collection);
            for(String mId : discardedMedia) {
            	Media media = mediaToBeClustered.get(mId);
            	deleteMedia(media);	
            }
        }
        
    }
    
    private Map<String, Double> getAvgDistances(AbstractClusterer clusterer, Cluster cluster) {
    	
    	double avgDistance = .0, textAvgDistance = .0, visualAvgDistance = .0;
    	DistanceFunction<Feature> textDistFuct = clusterer.getDistanceFunction("text");
    	DistanceFunction<Feature> visualDistFuct = clusterer.getDistanceFunction("visual");
    	
    	int tn = 0, vn = 0;
    	for(Instance instance1 : cluster.getMembers()) {
    		Feature textFtr1 = instance1.getFeature("text");
    		Feature visualFtr1 = instance1.getFeature("visual");
    		for(Instance instance2 : cluster.getMembers()) {
    			Feature textFtr2 = instance2.getFeature("text");
    			Feature visualFtr2 = instance2.getFeature("visual");
    			if(instance1 != instance2) {
    				avgDistance += clusterer.distance(instance1, instance2);
    				
    				if(textFtr1 != null && textFtr2 != null) {
    					tn++;
    					textAvgDistance += textDistFuct.distance(textFtr1, textFtr1);
    				}
    				if(visualFtr1 != null && visualFtr2 != null) {
    					vn++;
    					visualAvgDistance += visualDistFuct.distance(visualFtr1, visualFtr2);
    				}
    			}
    		}
    	}
    	
    	int n = cluster.getMembers().size();
    	
    	Map<String, Double> distances = new HashMap<String, Double>();
    	distances.put("avgDistance", avgDistance / (n*(n-1)));
    	
    	distances.put("textAvgDistance", textAvgDistance / (tn == 0 ? 1 : tn));
    	distances.put("visualAvgDistance", visualAvgDistance / (vn == 0 ? 1 : vn));
    	
    	return distances;
    }
    
    public void saveCluster(Cluster cluster, Map<String, Media> mediaToBeClustered, String centroidId, Map<String, Double> avgDistances) {
   		gr.iti.mklab.simmo.core.cluster.Cluster simmoCluster = new gr.iti.mklab.simmo.core.cluster.Cluster();
		simmoCluster.setId(cluster.getId());
		
		simmoCluster.setAvgDistances(avgDistances);
		
		if(centroidId != null) {
			Media mediaCentroid = mediaToBeClustered.get(centroidId);
			if(mediaCentroid != null) {
				Map<String, String> hm = new HashMap<String, String>();
				hm.put("id", centroidId);
				if (mediaCentroid instanceof Image) {
					hm.put("type", "image");
				}
				else {
					hm.put("type", "video");
				}
				simmoCluster.addCentroid(hm);
			}
		}
		
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
		try {
			clusterDAO.save(simmoCluster);
		}
		catch(Exception e) {
			LOGGER.error("Exception during cluster saving: " + e.getMessage());
		}
    }
    
    public void updateCluster(Cluster cluster, List<Media> newMembers, String centroidId, Map<String, Double> avgDistances) {
   	
		Query<gr.iti.mklab.simmo.core.cluster.Cluster> query = clusterDAO.createQuery().filter("_id", cluster.getId());
		
		UpdateOperations<gr.iti.mklab.simmo.core.cluster.Cluster> ops = clusterDAO.createUpdateOperations();
		
		ops.addAll("members", newMembers, false);
		ops.inc("size", newMembers.size());
		ops.set("avgDistances", avgDistances);	
		
		if(centroidId != null) {
			String type = null;
			for(Media media : newMembers) {
				if(centroidId.equals(media.getId())) {
					if (media instanceof Image) {
						type = "image";
					} else {
						type = "video";
					}
				}
			}
			if(type != null) {
				Map<String, String> hm = new HashMap<String, String>();
				hm.put("id", centroidId);
				hm.put("type", type);
				ops.add("centroids", hm, false);
			}
		}
		
		try {
			clusterDAO.update(query, ops);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
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
    
    public String getClusterCentroid(Cluster cluster, Clusterer clusterer) {
    	Instance centroidInstance = new Instance();
    	for(String type : cluster.getCentroids().keySet()) {
    		Feature centroid = cluster.getCentroids().get(type).getCentroid();
    		centroidInstance.addFeature(centroid);
    	}
    	
    	double bestDistance = Double.MAX_VALUE;
    	String centroidId = null;
    	
    	for(Instance member : cluster.getMembers()) {
    		double distance = clusterer.distance(member, centroidInstance);
    		if(distance < bestDistance) {
    			bestDistance = distance;
    			centroidId = member.getId();
    		}
    	}
    	
    	return centroidId;
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
    
    public static void main(String...args) throws ConfigurationException, IOException {
    	
    	Configuration.load(IncrementalClusterer.class.getResourceAsStream("/remote.properties"));
       
        // initialize mongodb
        MorphiaManager.setup("160.40.51.20");
    	
        
        IncrementalClusterer cl = new IncrementalClusterer("test_thresh",  0.9, 0.35, 0.55, CLUSTERER_TYPE.THRESHOLD);
    	
        cl.imageDAO.removeClusterAnnotations();
        cl.videoDAO.removeClusterAnnotations();
        
        cl.clusterDAO.deleteByQuery(cl.clusterDAO.createQuery());
        cl.run();
        
    }
   
}
