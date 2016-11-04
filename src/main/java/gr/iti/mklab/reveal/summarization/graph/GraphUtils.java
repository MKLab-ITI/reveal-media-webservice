package gr.iti.mklab.reveal.summarization.graph;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.collections15.Predicate;
import org.apache.commons.collections15.Transformer;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.jung.algorithms.filters.EdgePredicateFilter;
import edu.uci.ics.jung.algorithms.filters.Filter;
import edu.uci.ics.jung.algorithms.filters.VertexPredicateFilter;
import edu.uci.ics.jung.graph.DirectedGraph;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.UndirectedSparseGraph;
import edu.uci.ics.jung.io.GraphIOException;
import edu.uci.ics.jung.io.GraphMLWriter;
import edu.uci.ics.jung.io.graphml.EdgeMetadata;
import edu.uci.ics.jung.io.graphml.GraphMLReader2;
import edu.uci.ics.jung.io.graphml.GraphMetadata;
import edu.uci.ics.jung.io.graphml.HyperEdgeMetadata;
import edu.uci.ics.jung.io.graphml.NodeMetadata;
import edu.uci.ics.jung.io.graphml.GraphMetadata.EdgeDefault;


public class GraphUtils {

	
	public static  <V> Pair<Double, Double> getMinMaxWeight(Graph<V, Edge> graph) {
		Double min = Double.MAX_VALUE, max = .0;
		for(Edge e : graph.getEdges()) {
			Double weight = e.getWeight();
			if(weight > max) {
				max = weight;
			}
			if(weight < min) {
				min = weight;
			}
		}
		return Pair.of(min, max);
	}
	
	public static  <V> Double getAvgWeight(Graph<V, Edge> graph) {
		Double avg = .0;
		if(graph.getEdgeCount() > 0) {
			for(Edge e : graph.getEdges()) {
				Double weight = e.getWeight();
				avg += weight;
			}
			avg = avg / graph.getEdgeCount();
		}
		return avg;
	}
	
	public static <V, E> Double getGraphDensity(Graph<V, E> graph) {
		double v = (double) graph.getVertexCount();
		double e = 2. * (double) graph.getEdgeCount();
		
		if(v == 1) {
			return 1.;
		}
		
		return e / (v*(v-1));
	}
	
	public static <V> Map<Double, Double> getWeightDitribution(Graph<V, Edge> graph) {
		DecimalFormat df = new DecimalFormat("#.#");      
		Map<Double, Double> map = new TreeMap<Double, Double>();
		for(Edge e : graph.getEdges()) {
			Double weight = e.getWeight();
			weight = Double.valueOf(df.format(weight));
			
			Double freq = map.get(weight);
			if(freq == null) {
				freq = .0;
			}
			map.put(weight, ++freq);
		}
		Double sum = .0;
		for(Double freq : map.values()) {
			sum += freq;
		}
		for(Double bin : map.keySet()) {
			Double freq = map.get(bin);
			if(freq == null) {
				freq = .0;
			}
			else {
				freq = freq / sum;
			}
			map.put(bin, freq);
		}
		
		return map;
	}
	
	/*
	 * Get a directed graph of items from an undirected graph. 
	 * For each undirected edges two directed edges are added to the new graph.
	 */
	public static <V> DirectedGraph<V, Edge> toDirected(Graph<V, Edge> graph) {	
		DirectedGraph<V, Edge> directedGraph = new DirectedSparseGraph<V, Edge>();
	
		// Add all vertices first
		Collection<V> vertices = graph.getVertices();
		for(V vertex : vertices) {
			directedGraph.addVertex(vertex);
		}
		
		// Add directed edges
		for(Edge edge : graph.getEdges()) {	
			edu.uci.ics.jung.graph.util.Pair<V> endpoints = graph.getEndpoints(edge);
			directedGraph.addEdge(new Edge(edge.getWeight()), endpoints.getFirst(), endpoints.getSecond());
			directedGraph.addEdge(new Edge(edge.getWeight()), endpoints.getSecond(), endpoints.getFirst());
		}
		return directedGraph;
	}
	
	/*
	 * Get a directed graph of items from an undirected graph. 
	 * For each undirected edges two directed edges are added to the new graph.
	 */
	public static <V> DirectedGraph<V, Edge> toDirected(Graph<V, Edge> graph, Map<String, Long> order) {	
		DirectedGraph<V, Edge> directedGraph = new DirectedSparseGraph<V, Edge>();
	
		// Add all vertices first
		Collection<V> vertices = graph.getVertices();
		for(V vertex : vertices) {
			directedGraph.addVertex(vertex);
		}
		
		// Add directed edges
		for(Edge edge : graph.getEdges()) {	
			edu.uci.ics.jung.graph.util.Pair<V> endpoints = graph.getEndpoints(edge);
			
			Long firstOrder = order.get(endpoints.getFirst());
			Long secondOrder = order.get(endpoints.getSecond());
			if(firstOrder > secondOrder) {
				directedGraph.addEdge(new Edge(edge.getWeight()), endpoints.getFirst(), endpoints.getSecond());
			}
			else if(firstOrder < secondOrder) {
				directedGraph.addEdge(new Edge(edge.getWeight()), endpoints.getSecond(), endpoints.getFirst());
			}
			else {
				directedGraph.addEdge(new Edge(edge.getWeight()), endpoints.getFirst(), endpoints.getSecond());
				directedGraph.addEdge(new Edge(edge.getWeight()), endpoints.getSecond(), endpoints.getFirst());
			}
		}
		return directedGraph;
	}
	
	/*
	 * Normalize tha weights of edges in an undirected graph.
	 * The sum of weights of out-edges of a vertex has to be equal to 1.  
	 */
	public static <V> Graph<V, Edge> normalize(Graph<V, Edge> graph) {
		Graph<V, Edge> normalizedGraph = new DirectedSparseGraph<V, Edge>();
		
		Collection<V> vertices = graph.getVertices();
		for(V vertex : vertices) {
			normalizedGraph.addVertex(vertex);
		}
		
		for(V vertex : vertices) {		
			Collection<V> successors = graph.getSuccessors(vertex);
			
			double totalWeight = 0;	
			for(V successor : successors) {
				Edge edge = graph.findEdge(vertex, successor);
				if(edge != null) {
					totalWeight += edge.getWeight();
				}
			}
			
			if(totalWeight == 0)
				continue;
	
			for(V successor : successors) {
				Edge edge = graph.findEdge(vertex, successor);
				if(edge == null)
					continue;
				
				Double normalizedWeight = edge.getWeight() / totalWeight;
				
				Edge normalizedEdge = new Edge(normalizedWeight);
				normalizedGraph.addEdge(normalizedEdge, vertex, successor);
			}
		}
		
		return normalizedGraph;
	}
	
	public static  <V, E> Graph<V, E> clone(Graph<V, E> graph) {
		graph = filter(graph, graph.getVertices());
		return graph;
	}
	
	/*
	 * Filter graph vertices based on a set of desired vertices
	 */
	public static  <V, E> Graph<V, E> filter(Graph<V, E> graph, final Collection<V> vertices) {	
		Predicate<V> predicate = new Predicate<V>() {
			@Override
			public boolean evaluate(V vertex) {
				if(vertices.contains(vertex))
					return true;
			
				return false;
			}
		};
	
		//Filter graph
		Filter<V, E> verticesFilter = new VertexPredicateFilter<V, E>(predicate);
		graph = verticesFilter.transform(graph);

		return graph;
	}
	
	/*
	 * Filter graph vertices based on a set of desired vertices
	 */
	public static  <V, E> Graph<V, E> discardNodes(Graph<V, E> graph, final Collection<V> vertices) {	
		Predicate<V> predicate = new Predicate<V>() {
			@Override
			public boolean evaluate(V vertex) {
				if(vertices.contains(vertex)) {
					return false;
				}
				return true;
			}
		};
	
		//Filter graph
		Filter<V, E> verticesFilter = new VertexPredicateFilter<V, E>(predicate);
		graph = verticesFilter.transform(graph);

		return graph;
	}
	
	/*
	 * Filter graph vertices based on their degree 
	 */
	public static  <V> Graph<V, Edge> filter(final Graph<V, Edge> graph, final int degree) {
		Predicate<V> predicate = new Predicate<V>() {
			@Override
			public boolean evaluate(V vertex) {
				Collection<Edge> incidentEdges = graph.getIncidentEdges(vertex);
				if(incidentEdges.size() > degree) {
					return true;
				}
				return false;
			}
		};
	
		//Filter graph
		Filter<V, Edge> verticesFilter = new VertexPredicateFilter<V, Edge>(predicate);
		return verticesFilter.transform(graph);
		
	}
	
	/*
	 * Filter graph edges based on their weights 
	 */
	public static  <V> Graph<V, Edge> filter(Graph<V, Edge> graph, final double weightThreshold) {
		
		Predicate<Edge> edgePredicate = new Predicate<Edge>() {
			@Override
			public boolean evaluate(Edge edge) {
				if(edge.getWeight() > weightThreshold)
					return true;
			
				return false;
			}
		};
	
		//Filter graph
		Filter<V, Edge> edgeFiler = new EdgePredicateFilter<V, Edge>(edgePredicate);
		graph = edgeFiler.transform(graph);

		return graph;
	}
	
	/**
	 * Save graph in a .graphml file.
	 * 
	 * @param grap : the graph  to be saved
	 * @param filename : the name of the file
	 * @throws IOException
	 */
	public static void saveGraph(Graph<String, Edge> graph, String filename) throws IOException {
		
		File file =new File(filename);
		File dir = file.getParentFile();
		if(!dir.exists()) {
			dir.mkdirs();
		}
		
		GraphMLWriter<String, Edge> graphWriter = new GraphMLWriter<String, Edge> ();
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file)));
		
		graphWriter.addEdgeData("weight", null, "0", new Transformer<Edge, String>() {
				@Override
				public String transform(Edge e) {
					return Double.toString(e.getWeight());
				}
			}	
		);
		graphWriter.save(graph, out);
		
	}
	
	/**
	 * Save graph in a .graphml file.
	 * 
	 * @param grap : the graph  to be saved
	 * @param filename : the name of the file
	 * @throws IOException
	 */
	public static void saveProcessedGraph(Graph<Vertex, Edge> graph, Collection<Collection<String>>  clusters,
			Map<String, Double> priors, String filename) throws IOException {
		
		Map<String, String> associations = new HashMap<String, String>();
		int c = 0;
		for(Collection<String> cluster : clusters) {
			for(String member : cluster) {
				associations.put(member, Integer.toString(c));
			}
			c++;
		}
		
		File file =new File(filename);
		File dir = file.getParentFile();
		if(!dir.exists()) {
			dir.mkdirs();
		}
		
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file)));
		ExtendedGraphMLWriter<Vertex, Edge> graphWriter = new ExtendedGraphMLWriter<Vertex, Edge> ();
		
		graphWriter.addVertexData("weight", "double", "0", 				
			new Transformer<Vertex, String>() {
				@Override
				public String transform(Vertex v) {
					return Double.toString(v.getWeight());
				}
			}
		);
		graphWriter.addVertexData("prior", "double", "0", 				
				new Transformer<Vertex, String>() {
					@Override
					public String transform(Vertex v) {
						String id = v.getId();
						Double prior = priors.get(id);
						
						return Double.toString(prior==null ? .0 : prior);
					}
				}
			);
		graphWriter.addVertexData("cluster", "string", "NULL", 				
				new Transformer<Vertex, String>() {
					@Override
					public String transform(Vertex v) {
						String cluster = associations.get(v.getId());
						return cluster;
					}
				}
			);
		
		graphWriter.addEdgeData("weight", "double", "0", 
			new Transformer<Edge, String>() {
				@Override
				public String transform(Edge e) {
					return Double.toString(e.getWeight());
				}
			}
		);

		graphWriter.save(graph, out);

		
	}
	
	/**
	 * Load a graph from a .graphml file
	 * @param filename : the name of the file that the graph is stored
	 * @return graph
	 * @throws IOException
	 */
	public static Graph<String, Edge> loadGraph(String filename) throws IOException {
		
		BufferedReader fileReader = new BufferedReader(new FileReader(filename));	
		
		Transformer<GraphMetadata, Graph<String, Edge>> graphTransformer = new Transformer<GraphMetadata, Graph<String, Edge>>() {
			public Graph<String, Edge> transform(GraphMetadata metadata) {
				if (metadata.getEdgeDefault().equals(EdgeDefault.DIRECTED)) {
					return new DirectedSparseGraph<String, Edge>();
				} else {
					return new UndirectedSparseGraph<String, Edge>();
				}
			}
		};
		
		Transformer<NodeMetadata, String> vertexTransformer = new Transformer<NodeMetadata, String>() {
			public String transform(NodeMetadata metadata) {
				String vertex = metadata.getId();
				return vertex;
			}
		};
			
		Transformer<EdgeMetadata, Edge> edgeTransformer = new Transformer<EdgeMetadata, Edge>() {
			int e = 0;
			public Edge transform(EdgeMetadata metadata) {
				if(++e%500000 == 0) {
					System.out.print(".");
				}
				
				Double weight = Double.parseDouble(metadata.getProperty("weight"));
				Edge edge = new Edge(weight);
				return edge;
			}
		};
		
		Transformer<HyperEdgeMetadata, Edge> hyperEdgeTransformer = new Transformer<HyperEdgeMetadata, Edge>() {
			public Edge transform(HyperEdgeMetadata metadata) {
				Double weight = Double.parseDouble(metadata.getProperty("weight"));
				
				Edge edge = new Edge(weight);
				return edge;
			}
		};
					 
		GraphMLReader2<Graph<String, Edge>, String, Edge> graphReader 
			= new GraphMLReader2<Graph<String, Edge>, String, Edge>(
				fileReader, graphTransformer, vertexTransformer, edgeTransformer, hyperEdgeTransformer);

		try {
			/* 
			 * Get the new graph object from the GraphML file 
			 * */
			Graph<String, Edge> graph = graphReader.readGraph();
			System.out.println(".");
			return graph;
		} catch (GraphIOException ex) {
			return null;
		}
	}
	
	
	/**
	 * Folds sub-graphs of the graph into single nodes based on a clustering of nodes.
	 * Does not return a new graph but changes the initial graph passed as a parameter
	 * 
	 * @param graph: a graph of items
	 * @param clusters: a collections of clustered items 
	 */
	public static void fold(Graph<String, Edge> graph, Collection<Collection<String>> clusters) {
		
		int clustered = 0, removedEdges=0;
		for(Collection<String> cluster : clusters) {
			
			List<String> list = new ArrayList<String>(cluster);
			Collections.sort(list);
			String newVertex = StringUtils.join(list, "-");
			
			//System.out.println("Cluster " + clusters.indexOf(cluster) + " size " + cluster.size());
			clustered += cluster.size();
			
			for(String v1 : cluster) {
				for(String v2 : cluster) {
					Edge edge = graph.findEdge(v1, v2);
					if(edge != null) {
						removedEdges++;
						graph.removeEdge(edge);
					}
				}
			}
			//System.out.println("Between edges to remove:  " + edgesToRemove);

			Map<String, Set<Edge>> map = new HashMap<String, Set<Edge>>();
			
			for(String vertex : cluster) {
				Collection<String> neighbors = new ArrayList<String>(graph.getNeighbors(vertex));
				//System.out.println(vertex + " => " + neighbors.size());
				for(String neighbor : neighbors) {
					Edge edge = graph.findEdge(vertex, neighbor);
					if(edge != null) {
						removedEdges++;
						graph.removeEdge(edge);
						Set<Edge> edges = map.get(neighbor);
						if(edges == null) {
							edges = new HashSet<Edge>();
							map.put(neighbor, edges);
						}
						edges.add(edge);
					}
				}
				graph.removeVertex(vertex);
			}
			
			
			graph.addVertex(newVertex);
			
			for(String neighbor : map.keySet()) {
				Set<Edge> edges = map.get(neighbor);
				Edge maxEdge = Collections.max(edges);
				graph.addEdge(maxEdge, neighbor, newVertex);
			}
		}
		
		System.out.println("Clustered Vertices: " + clustered + ", Removed Edges: " + removedEdges);
	}
}
