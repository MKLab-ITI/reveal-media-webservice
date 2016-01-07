package gr.iti.mklab.reveal.summarization;

import edu.uci.ics.jung.graph.DirectedGraph;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.UndirectedSparseGraph;
import gr.iti.mklab.reveal.util.Configuration;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.morphia.MediaDAO;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import info.debatty.java.graphs.Neighbor;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.java.graphs.build.ThreadedNNDescent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.apache.commons.configuration.ConfigurationException;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;

public class MediaSummarizer implements Callable<List<RankedImage>> {

	private final static int ITEMS_PER_ITERATION = 2000;
	
	private String collection;

	private double similarityCuttof = 0.2;
			
	public MediaSummarizer(String collection) {
		try {
			Configuration.load(getClass().getResourceAsStream("/remote.properties"));
		} catch (ConfigurationException | IOException e) {
			e.printStackTrace();
		}
		this.collection = collection;
	}
	
	@Override
	public List<RankedImage> call() throws Exception {
		
		MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, collection);
		DAO<RankedImage, String> rankedImagesDAO = new BasicDAO<RankedImage, String>(
				RankedImage.class, 
				MorphiaManager.getMongoClient(), 
				MorphiaManager.getMorphia(), 
				MorphiaManager.getDB(collection).getName()
			);
        
		Graph<String, Edge> graph = new UndirectedSparseGraph<String, Edge>();

        Map<String, String> texts = new HashMap<String, String>();
        Map<String, Integer> popularities = new HashMap<String, Integer>();
		for (int k = 0; k < imageDAO.count(); k += ITEMS_PER_ITERATION) {
            List<Image> images = imageDAO.getItems(ITEMS_PER_ITERATION, k);
            images.stream().forEach(image -> {
            	graph.addVertex(image.getId());
            	
            	String title = image.getTitle();
            	if(title != null && !title.equals("")) {
            		texts.put(image.getId(), title);
            	}
            	
            	int popularity = image.getNumShares() + image.getNumLikes() + image.getNumComments()
            			+ image.getNumViews();
            	
            	popularities.put(image.getId(), popularity);
            });
        }
		Map<String, Vector> vectorsMap = Vocabulary.createVocabulary(texts, 2);
		
		System.out.println(graph.getVertexCount());
		
		int k = 0;
		double ratio = 0.01;
		while(k == 0 && ratio <=1) {
			k = (int) (ratio * graph.getVertexCount());
			ratio += 0.01;
		}
		
		System.out.println("K=" + k);
		
        ThreadedNNDescent<Vector> builder = new ThreadedNNDescent<Vector>();
        builder.setThreadCount(8);
        builder.setK(k);
        builder.setDelta(0.05);
        builder.setRho(0.5);
        builder.setMaxIterations(20);
        
        builder.setSimilarity(new SimilarityInterface<Vector>() {
        	
			private static final long serialVersionUID = -2703570812879875880L;
			@Override
            public double similarity(Vector v1, Vector v2) {
				if(v1 != null && v2 != null) {
					try {
						double similarity = v1.cosine(v2);
						if(similarity < similarityCuttof) {
							return .0;
						}
						
						return similarity;
					} catch (Exception e) {}
				}
				return .0;
            }
        });
        

        List<Node<Vector>> nodes = new ArrayList<Node<Vector>>();
        for (String vertex : graph.getVertices()) {
        	Vector vector = vectorsMap.get(vertex);
        	if(vector != null) {
        		Node<Vector> node = new Node<Vector>(vertex, vector);
        		nodes.add(node);
        	}
        }

        Map<Node<Vector>, NeighborList> nn = builder.computeGraph(nodes);
        for(Node<Vector> node : nn.keySet()) {
        	String v1 = node.id;
        	NeighborList nl = nn.get(node);
        
        	Iterator<Neighbor> it = nl.iterator();
        	while(it.hasNext()) {
        		Neighbor neighbor = it.next();
        		String v2 = neighbor.node.id;
        		Edge edge = graph.findEdge(v1, v2);
        		if(edge == null && neighbor.similarity > similarityCuttof) {
        			edge = new Edge(neighbor.similarity);
        			graph.addEdge(edge, v1, v2);
        		}
        	}
        }    

        System.out.println("Graph created: " + graph.getVertexCount() + " vertices and " 
        		+ graph.getEdgeCount() + " edges. Density: " + GraphUtils.getGraphDensity(graph));
        
        Collection<Collection<String>> clusters = GraphClusterer.cluster(graph, true);
        System.out.println(clusters.size() + " clusters detected.");
        
        Map<String, Double> priors = getPriorScores(graph.getVertices(), popularities, clusters); 
        
		DirectedGraph<String, Edge> directedGraph = GraphUtils.toDirected(graph);
		Graph<String, Edge> normalizedGraph = GraphUtils.normalize(directedGraph);
		
		Map<String, Double> divrankScores = GraphRanker.divrankScoring(normalizedGraph, priors);
		  
		List<RankedImage> rankedImages = new ArrayList<RankedImage>();
		for(Entry<String, Double> entry : divrankScores.entrySet()) {
			RankedImage rankedImage = new RankedImage(entry.getKey(), entry.getValue());
			rankedImages.add(rankedImage);
			
			rankedImagesDAO.save(rankedImage);
		}
		
		return rankedImages;
	}
	
	public Map<String, Double> getPriorScores(Collection<String> ids, Map<String, Integer> popularities,
			 Collection<Collection<String>> clusters) {
		
		Map<String, Integer> topicSignificane = new HashMap<String, Integer>();
		for(Collection<String> cluster : clusters) {
			for(String member : cluster) {
				topicSignificane.put(member, cluster.size());	
			}
		}
		
		Map<String, Double> scores = new HashMap<String, Double>();
		
		Integer maxPop = Collections.max(popularities.values());
		if(maxPop == null || maxPop == 0) {
			maxPop = 1;
		}
		
		Integer maxCluster = Collections.max(topicSignificane.values());
		if(maxCluster == null || maxCluster == 0) {
			maxCluster = 1;
		}
		
		for(String id : ids) {
			Double score = .0;
			Integer p = popularities.get(id);
			if(p == null) {
				p = 0;
			}
			Double popularity = Math.log(Math.E + (p.doubleValue() / maxPop.doubleValue()));
			
			Integer sig = topicSignificane.get(id);
			if(sig == null) {
				sig = 0;
			}
			
			Double significance = Math.exp(sig.doubleValue() / maxCluster.doubleValue());
			score = popularity * significance;
			
			scores.put(id, score);
		}
		
		return scores;
	}
	
	public static void main(String[] args) throws Exception {
		
		MorphiaManager.setup("160.40.51.20");
		
		MediaSummarizer sum = new MediaSummarizer("tsipras");
		System.out.println("Run summarizer!");
		sum.call();
	}
}
