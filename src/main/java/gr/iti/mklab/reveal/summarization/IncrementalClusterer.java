package gr.iti.mklab.reveal.summarization;

import gr.iti.mklab.reveal.summarization.graph.Edge;
import gr.iti.mklab.reveal.summarization.graph.GraphClusterer;
import gr.iti.mklab.reveal.summarization.graph.GraphUtils;
import gr.iti.mklab.reveal.util.Configuration;
import gr.iti.mklab.reveal.visual.VisualIndexClient;
import gr.iti.mklab.simmo.core.cluster.Cluster;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.items.Video;
import gr.iti.mklab.simmo.core.morphia.MediaDAO;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;

import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.UndirectedSparseGraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A clustering runnable which clusters the given number of images and videos from
 * the specified collection using the supplied configuration options.
 *
 * @author kandreadou
 */
public class IncrementalClusterer implements Runnable {

	private Logger _logger = Logger.getLogger(IncrementalClusterer.class);
			
    private String collection;

    private boolean isRunning = true;
    
    private int count = 100;
	private double textSimilarityCuttof = 0.2;
	private double visualSimilarityCuttof = 0.25;
	private int mu = 4;
	private double epsilon = 0.7;
	
	private MediaDAO<Image> imageDAO;
	private MediaDAO<Video> videoDAO;
    
    public IncrementalClusterer(String collection) {
        this.collection = collection;
        
        this.imageDAO = new MediaDAO<Image>(Image.class, collection);
        this.videoDAO = new MediaDAO<Video>(Video.class, collection);
        
        GraphClusterer.scanMu = mu;
    	GraphClusterer.scanEpsilon = epsilon;
    }

    @Override
    public void run() {
    	
		Graph<Vertex, Edge> graph = new UndirectedSparseGraph<Vertex, Edge>();

        String indexServiceHost = "http://" + Configuration.INDEX_SERVICE_HOST + ":8080/VisualIndexService";
		VisualIndexClient vIndexClient = new VisualIndexClient(indexServiceHost, collection);  
    
        //First get the existing clusters for this collection
        DAO<Cluster, String> clusterDAO = new BasicDAO<>(Cluster.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB(collection).getName());
        
        while(isRunning) {
        	List<Cluster> savedClusters = clusterDAO.find().asList();
            
        	List<String> mediaToBeClustered = new ArrayList<String>();
        	Map<String, String> texts = new HashMap<String, String>();
            Map<String, Double[]> visualVectors = new HashMap<String, Double[]>();
            
            List<Image> images = imageDAO.getIndexedNotClustered(count);
            images.stream().forEach(i -> {
            	mediaToBeClustered.add(i.getId());
                Double[] vector = vIndexClient.getVector(i.getId());
                if (vector != null && vector.length == 1024) {
                	visualVectors.put(i.getId(), vector);
                	texts.put(i.getId(), i.getTitle());
                }
            });
            
            List<Video> videos = videoDAO.getIndexedNotClustered(count);
            videos.stream().forEach(i -> {
            	mediaToBeClustered.add(i.getId());
            	Double[] vector = vIndexClient.getVector(i.getId());
                if (vector != null && vector.length == 1024) {
                	visualVectors.put(i.getId(), vector);
                	texts.put(i.getId(), i.getTitle());
                }
            });
        	
            Map<String, Vector> vectorsMap = Vocabulary.createVocabulary(texts, 2);
            
            for(String id : mediaToBeClustered) {
            	Vertex vertex = new Vertex(id, visualVectors.get(id), vectorsMap.get(id));
            	graph.addVertex(vertex);
            }
            
            List<Vertex> vertices = new ArrayList<Vertex>(graph.getVertices());
            for(int i=0; i < graph.getVertexCount()-1; i++) {
            	Vertex v1 = vertices.get(i);
            	for(int j=i+1; j < graph.getVertexCount(); j++) {
            		Vertex v2 = vertices.get(j);
            		if(!graph.isNeighbor(v1, v2)) {
            			Pair<Double, Double> similarities = v1.similarities(v2);
            			if(textSimilarityCuttof < similarities.getLeft() && visualSimilarityCuttof < similarities.getRight()) {
            				double w = similarities.getLeft() + similarities.getRight();
            				graph.addEdge(new Edge(w), v1, v2);
            			}
            		}
                }	
            }
            
        	Collection<Collection<Vertex>> clusters = GraphClusterer.cluster(graph, false);
        	
        }
        
    }

}
