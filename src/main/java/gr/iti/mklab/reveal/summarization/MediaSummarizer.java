package gr.iti.mklab.reveal.summarization;

import edu.uci.ics.jung.graph.DirectedGraph;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.UndirectedSparseGraph;
import gr.iti.mklab.reveal.summarization.graph.Edge;
import gr.iti.mklab.reveal.summarization.graph.GraphClusterer;
import gr.iti.mklab.reveal.summarization.graph.GraphRanker;
import gr.iti.mklab.reveal.summarization.graph.GraphUtils;
import gr.iti.mklab.reveal.summarization.utils.L2;

import gr.iti.mklab.reveal.visual.VisualIndexer;
import gr.iti.mklab.reveal.visual.VisualIndexerFactory;
import gr.iti.mklab.simmo.core.annotations.SummaryScore;
import gr.iti.mklab.simmo.core.cluster.Cluster;
import gr.iti.mklab.simmo.core.cluster.Clusterable;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.morphia.MediaDAO;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import info.debatty.java.graphs.Neighbor;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.java.graphs.build.ThreadedNNDescent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.apache.commons.lang.ArrayUtils;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.UpdateOperations;

public class MediaSummarizer implements Callable<List<RankedImage>> {

	private final static int ITEMS_PER_ITERATION = 2000;
	
	private String collection;

	private double textSimilarityCuttof = 0.2;
	private double visualSimilarityCuttof = 0.25;

	private double randomJumpWeight = 0.75;

	private int mu;
	private double epsilon;
			
	public MediaSummarizer(String collection, double similarityCuttof, double visualSimilarity, 
			double randomJumpWeight, int mu, double epsilon) {
		
		/*
		try {
			Configuration.load(getClass().getResourceAsStream("/remote.properties"));
		} catch (ConfigurationException | IOException e) {
			e.printStackTrace();
		}
		*/
		
		this.textSimilarityCuttof = similarityCuttof;
		this.visualSimilarityCuttof = visualSimilarity;
		this.randomJumpWeight  = randomJumpWeight;
		this.mu = mu;
		this.epsilon = epsilon;
		
		this.collection = collection;
	}
	
	@Override
	public List<RankedImage> call() throws Exception {
		
		List<RankedImage> rankedImages = new ArrayList<RankedImage>();
		
		DAO<Cluster, String> clusterDAO = new BasicDAO<>(
				Cluster.class, 
				MorphiaManager.getMongoClient(), 
				MorphiaManager.getMorphia(), 
				MorphiaManager.getDB(collection).getName()
			);
		 
		MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, collection);
		
		DAO<RankedImage, String> rankedImagesDAO = new BasicDAO<RankedImage, String>(
				RankedImage.class, 
				MorphiaManager.getMongoClient(), 
				MorphiaManager.getMorphia(), 
				MorphiaManager.getDB(collection).getName()
			);
		
		VisualIndexer.lightinit();
		VisualIndexer vIndexer = VisualIndexerFactory.getVisualIndexer(collection);
		
		Graph<String, Edge> graph = new UndirectedSparseGraph<String, Edge>();

        Map<String, String> texts = new HashMap<String, String>();
        Map<String, Long> times = new HashMap<String, Long>();
        Map<String, Integer> popularities = new HashMap<String, Integer>();
        Map<String, Double[]> visualVectors = new HashMap<String, Double[]>();
        
        long current = System.currentTimeMillis();
		for (int k = 0; k < imageDAO.count(); k += ITEMS_PER_ITERATION) {
            List<Image> images = imageDAO.getItems(ITEMS_PER_ITERATION, k);
            images.stream().forEach(image -> {
            	/*
            	List<? extends Annotation> annotations = image.getAnnotationsByClass(NamedEntity.class);
            	for(Annotation annotation : annotations) {
            		NamedEntity ne = (NamedEntity) annotation;
            		
            	}
            	Set<String> tags = image.getTags();
            	if(!tags.isEmpty()) {
            		for(String tag : tags) {
            			
            		}
            	}
            	*/
            	
            	graph.addVertex(image.getId());
            	
            	String title = image.getTitle();
            	if(title != null && !title.equals("")) {
            		texts.put(image.getId(), title);
            	}
            	
            	Date date = image.getCreationDate();
            	if(date == null || date.getTime() == 0) {
            		date = image.getLastModifiedDate();	
            	}
            	if(date != null && date.getTime() > 0) {
            		times.put(image.getId(), date.getTime());
            	}
            	else {
            		date = image.getCrawlDate();
            		if(date != null) {
            			times.put(image.getId(), date.getTime());
            		}
            		else { 
            			times.put(image.getId(), current);
            		}
            	}
            	
            	int popularity = image.getNumShares() + image.getNumLikes() 
            			+ image.getNumComments() + image.getNumViews();
            	
            	popularities.put(image.getId(), popularity);
            	
            	Double[] vector = vIndexer.getVector(image.getId());
            	if(vector != null && vector.length > 0) {
            		visualVectors.put(image.getId(), vector);	
            	}
            	
            });
        }
		
		try {
			Map<String, Vector> vectorsMap = Vocabulary.createVocabulary(texts, 2);
			System.out.println("Vocabulary created with " + Vocabulary.getNumOfTerms() + " terms.");
				 
			createGraph(graph, vectorsMap);
        	System.out.println("Graph created: " + graph.getVertexCount() + " vertices and " + graph.getEdgeCount() + " edges. Density: " + GraphUtils.getGraphDensity(graph));
        
        	attachVisualEdges(graph, visualVectors);
        	System.out.println("Graph created: " + graph.getVertexCount() + " vertices and " + graph.getEdgeCount() + " edges. Density: " + GraphUtils.getGraphDensity(graph));
        	
		}
		catch(Exception e) {
			System.out.println("MediaSUmmarizer => Exception during graph generation: " + e.getMessage());
			e.printStackTrace();
			return rankedImages;
		}
        
		
		Collection<Collection<String>> clusters = null;
		try {
			GraphClusterer.scanMu = mu;
        	GraphClusterer.scanEpsilon = epsilon;
        	clusters = GraphClusterer.cluster(graph, false);
        	System.out.println(clusters.size() + " clusters detected.");
        
        	for(Collection<String> c : clusters) {
        		Cluster cluster = new Cluster();
        		List<Clusterable> members = new ArrayList<Clusterable>();
        		for(String mId : c) {
        			Image image = imageDAO.get(mId);
        			if(image != null) {
        				members.add(image);
        			}
        		}
        		cluster.setMembers(members);
        		cluster.setSize(members.size());
        	
        		clusterDAO.save(cluster);
        	}
		}
		catch(Exception e) {
			System.out.println("MediaSummarizer => Exception during clustering: " + e.getMessage());
			e.printStackTrace();
			return rankedImages;
		}
        
        Map<String, Double> priors = getPriorScores(graph.getVertices(), popularities, clusters, graph); 
        
		DirectedGraph<String, Edge> directedGraph = GraphUtils.toDirected(graph, times);
		//Graph<String, Edge> normalizedGraph = GraphUtils.normalize(directedGraph);
		
		GraphRanker.d = randomJumpWeight;
		Map<String, Double> pagerankScores = GraphRanker.pagerankScoring(directedGraph, priors);
		Map<String, Double> divrankScores = GraphRanker.divrankScoring(directedGraph, priors);
		
		for(Entry<String, Double> entry : divrankScores.entrySet()) {
			RankedImage rankedImage = new RankedImage(entry.getKey(), entry.getValue());
			rankedImages.add(rankedImage);
			
			rankedImagesDAO.save(rankedImage);
		}
		
		for(String vertex : graph.getVertices()) {
			try {
				SummaryScore summaryScore = new SummaryScore(
					popularities.get(vertex), 
					priors.get(vertex), 
					pagerankScores.get(vertex), 
					divrankScores.get(vertex)
				);
			
				// add annotation to image
				Query<Image> query = imageDAO.createQuery().filter("_id", vertex);
				UpdateOperations<Image> ops = imageDAO.createUpdateOperations().add("annotations", summaryScore);
				imageDAO.update(query, ops);
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		//////////////////////////////////////////////////////////////////////////////
		//////////////////////////////////////////////////////////////////////////////
		//////////////////// E X P E R I M E N T A L /////////////////////////////////
		//////////////////////////////////////////////////////////////////////////////
		//////////////////////////////////////////////////////////////////////////////
		/*
		Graph<Vertex, Edge> weightedGraph = new UndirectedSparseGraph<Vertex, Edge>();
		for(RankedImage ri : rankedImages) {
			Vertex v = new Vertex(ri.getId(), ri.getScore());
			weightedGraph.addVertex(v);
		}
		
		List<Vertex> vertices = new ArrayList<Vertex>(weightedGraph.getVertices());
		for(int i = 0; i < (vertices.size()-1); i++) {
			Vertex v1 = vertices.get(i);
			for(int j = i+1; j<vertices.size(); j++) {
				Vertex v2 = vertices.get(j);
				Edge edge = graph.findEdge(v1.getId(), v2.getId());
				if(edge != null) {
					weightedGraph.addEdge(edge, v1, v2);
				}
			}	
		}
		GraphUtils.saveProcessedGraph(weightedGraph, clusters, priors, "./wg.graphml");
		*/
		///////////////////////////////////////////////////////////////////////////////
		///////////////////////////////////////////////////////////////////////////////
		//////////////////////////////////////////////////////////////////////////////
		//////////////////////////////////////////////////////////////////////////////
		
		return rankedImages;
	}
	
	public void createGraph(Graph<String, Edge> graph, Map<String, Vector> vectorsMap) {
		int k = 0;
		double ratio = 0.02;
		while(k == 0 && ratio <=1) {
			k = (int) (ratio * graph.getVertexCount());
			ratio += 0.01;
		}
		
		System.out.println("K = " + k);
		
        ThreadedNNDescent<Vector> builder = new ThreadedNNDescent<Vector>();
        
        builder.setThreadCount(8);
        builder.setK(k);
        builder.setDelta(0.00001);
        builder.setRho(0.5);
        builder.setMaxIterations(20);
        
        builder.setSimilarity(new SimilarityInterface<Vector>() {
        	
			private static final long serialVersionUID = -2703570812879875880L;
			@Override
            public double similarity(Vector v1, Vector v2) {
				if(v1 != null && v2 != null) {
					try {
						double similarity = v1.cosine(v2);
						if(similarity < 0) {
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
        //builder.test(nodes);
        System.out.println("Iterations: " + builder.getIterations());
        for(Node<Vector> node : nn.keySet()) {
        	String v1 = node.id;
        	NeighborList nl = nn.get(node);
        
        	Iterator<Neighbor> it = nl.iterator();
        	while(it.hasNext()) {
        		Neighbor neighbor = it.next();
        		if(neighbor == null) {
        			continue;
        		}
        		String v2 = neighbor.node.id;
        		Edge edge = graph.findEdge(v1, v2);
        		if(edge == null && neighbor.similarity > textSimilarityCuttof) {
        			edge = new Edge(neighbor.similarity);
        			graph.addEdge(edge, v1, v2);
        		}
        	}
        }    
	}
	
	public void attachVisualEdges(Graph<String, Edge> graph, Map<String, Double[]> visualVectors) {
		
		if(visualVectors.isEmpty()) {
			return;
		}
		
		int k = 0;
		double ratio = 0.02;
		while(k == 0 && ratio <= 1) {
			k = (int) (ratio * graph.getVertexCount());
			ratio += 0.01;
		}
		
        ThreadedNNDescent<Double[]> builder = new ThreadedNNDescent<Double[]>();
        builder.setThreadCount(8);
        builder.setK(k);
        builder.setDelta(0.00001);
        builder.setRho(0.5);
        builder.setMaxIterations(20);
        
        builder.setSimilarity(new SimilarityInterface<Double[]>() {
        	
			private static final long serialVersionUID = -2703570812879875880L;
			@Override
            public double similarity(Double[] v1, Double[] v2) {
				if(v1 != null && v2 != null) {
					try {
						double similarity = L2.similarity(ArrayUtils.toPrimitive(v1), ArrayUtils.toPrimitive(v2));
						return similarity;
					} catch (Exception e) {
						
					}
				}
				return .0;
            }
        });
        
        List<Node<Double[]>> nodes = new ArrayList<Node<Double[]>>();
        for (String vertex : visualVectors.keySet()) {
        	Double[] vector = visualVectors.get(vertex);
        	if(vector != null) {
        		Node<Double[]> node = new Node<Double[]>(vertex, vector);
        		nodes.add(node);
        	}
        }

        Map<Node<Double[]>, NeighborList> nn = builder.computeGraph(nodes);
        for(Node<Double[]> node : nn.keySet()) {
        	String v1 = node.id;
        	NeighborList nl = nn.get(node);
        
        	Iterator<Neighbor> it = nl.iterator();
        	while(it.hasNext()) {
        		Neighbor neighbor = it.next();
        		if(neighbor != null) {
        			String v2 = neighbor.node.id;
        			Edge edge = graph.findEdge(v1, v2);
        			if(edge == null && neighbor.similarity >= visualSimilarityCuttof) {
        				edge = new Edge(neighbor.similarity);
        				graph.addEdge(edge, v1, v2);
        			}
        		}
        	}
        }    
	}
	
	public Map<String, Double> getPriorScores(Collection<String> ids, Map<String, Integer> popularities, 
			Collection<Collection<String>> clusters, Graph<String, Edge> graph) {
			
		Map<String, Integer> topicSignificane = new HashMap<String, Integer>();
		Map<String, Double> topicDensities = new HashMap<String, Double>();
		
		for(Collection<String> cluster : clusters) {
			Graph<String, Edge> subGraph = GraphUtils.filter(graph, cluster);
			Double density = GraphUtils.getGraphDensity(subGraph);
			for(String member : cluster) {
				topicDensities.put(member, density);
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
			Double popularity = Math.log(Math.E + (1 + p.doubleValue() / maxPop.doubleValue()));
			
			Integer sig = topicSignificane.get(id);
			if(sig == null) {
				sig = 0;
			}
			Double significance = Math.exp(sig.doubleValue() / maxCluster.doubleValue());
			
			Double density = topicDensities.get(id);
			if(density == null) {
				density = 1.;
			}
			
			score = density * popularity * significance;
			
			scores.put(id, score);
		}
		
		return scores;
	}
	
	public static void main(String[] args) throws Exception {
		
		MorphiaManager.setup("160.40.51.20");
		
		MediaSummarizer sum = new MediaSummarizer("test", 0.65, 0.25, 0.75, 4, 0.7);
		System.out.println("Run summarizer!");
		sum.call();
	}
}
