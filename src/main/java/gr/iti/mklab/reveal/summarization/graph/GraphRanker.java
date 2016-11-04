package gr.iti.mklab.reveal.summarization.graph;

import edu.uci.ics.jung.algorithms.matrix.GraphMatrixOperations;
import edu.uci.ics.jung.algorithms.scoring.EigenvectorCentrality;
import edu.uci.ics.jung.algorithms.scoring.HITS.Scores;
import edu.uci.ics.jung.algorithms.scoring.PageRank;
import edu.uci.ics.jung.algorithms.scoring.HITS;
import edu.uci.ics.jung.algorithms.scoring.PageRankWithPriors;
import edu.uci.ics.jung.graph.Graph;
import edu.ucla.sspace.matrix.SparseHashMatrix;
import edu.ucla.sspace.matrix.SparseMatrix;
import edu.ucla.sspace.vector.DenseVector;
import edu.ucla.sspace.vector.DoubleVector;
import gr.iti.mklab.reveal.summarization.divrank.DivRank;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.collections15.Transformer;

import cern.colt.list.DoubleArrayList;
import cern.colt.list.IntArrayList;
import cern.colt.matrix.impl.SparseDoubleMatrix2D;

public class GraphRanker {
	
	//public static Double d = 0.75; //damping factor in PageRank
	public static Double d = 0.75; //damping factor in PageRank
	
	// Stoping criteria
	private static double tolerance = 0.000001;
	private static int maxIterations = 300;
	
	public static Map<String, Double> pagerankScoring(Graph<String, Edge>  graph) {
		
		Transformer<Edge, Double> edgeTransformer = Edge.getEdgeTransformer();
		PageRank<String, Edge> ranker = new PageRank<String, Edge>(graph, edgeTransformer , d);
		
		ranker.setTolerance(tolerance) ;
		ranker.setMaxIterations(maxIterations);
		ranker.evaluate();
	 
		System.out.println("Iterations: " + ranker.getIterations());
		System.out.println("Tolerance: " + ranker.getTolerance());
		
		double maxScore = 0;
		Collection<String> vertices = graph.getVertices();
		Map<String, Double> verticesMap = new TreeMap<String, Double>();
		for(String vertex : vertices) {
			Double score = ranker.getVertexScore(vertex);
			
			if(score > maxScore)
				maxScore = score;
			
			verticesMap.put(vertex, score);
		}
		
		if(maxScore > 0) {
			for(Entry<String, Double> ve : verticesMap.entrySet()) {
				verticesMap.put(ve.getKey(), ve.getValue()/maxScore);
			}
		}
		
		return verticesMap;
	}
	
	public static Map<String, Double> pagerankScoring(Graph<String, Edge>  graph, final Map<String, Double> priors) {
		
		Transformer<Edge, Double> edgeTransformer = Edge.getEdgeTransformer();
		Transformer<String, Double> priorsTransformer = new Transformer<String, Double>() {
			@Override
			public Double transform(String vertex) {
				Double vertexPrior = priors.get(vertex);
				if(vertexPrior == null) {
					return 0d;
				}
				
				return vertexPrior;
			}
		};
		
		PageRankWithPriors<String, Edge> ranker = 
				new PageRankWithPriors<String, Edge>(graph, edgeTransformer, priorsTransformer, d);
		
		ranker.setTolerance(tolerance) ;
		ranker.setMaxIterations(maxIterations);
		ranker.evaluate();
	
		double maxScore = 0;
		Collection<String> vertices = graph.getVertices();
		Map<String, Double> verticesMap = new TreeMap<String, Double>();
		for(String vertex : vertices) {
			Double score = ranker.getVertexScore(vertex);
			
			if(score > maxScore) {
				maxScore = score;
			}
			
			verticesMap.put(vertex, score);
		}
	
		if(maxScore > 0) {
			for(Entry<String, Double> ve : verticesMap.entrySet()) {
				verticesMap.put(ve.getKey(), ve.getValue()/maxScore);
			}
		}
		
		return verticesMap;
	}
	
	public static Map<String, Double> hitsScoring(Graph<String, Edge>  graph) {

		HITS<String, Edge> ranker = new HITS<String, Edge>(graph, d);
		ranker.setTolerance(tolerance) ;
		ranker.setMaxIterations(maxIterations);
		ranker.evaluate();
	
		Collection<String> vertices = graph.getVertices();
		Map<String, Double> verticesMap = new TreeMap<String, Double>();
		for(String vertex : vertices) {
			Scores hitsScores = ranker.getVertexScore(vertex);
			Double authorityScore = hitsScores.authority;
			//Double hubScore = hitsScores.hub;	
			verticesMap.put(vertex, authorityScore);
			//temp.put(vertex, hubScore);
		}
		return verticesMap;
	}

	public static TreeMap<String, Double> eigenvectorScoring(Graph<String, Edge>  graph) {
		
		EigenvectorCentrality<String, Edge> ranker = 
				new EigenvectorCentrality<String, Edge>(graph, Edge.getEdgeTransformer());
		ranker.evaluate();
		
		Collection<String> vertices = graph.getVertices();
		TreeMap<String, Double> verticesMap = new TreeMap<String, Double>();
		for(String vertex : vertices) {
			Double score = ranker.getVertexScore(vertex);
			verticesMap.put(vertex, score);
		}
		
		return verticesMap;
	}
	
	public static Map<String, Double> divrankScoring(Graph<String, Edge>  graph) {
		
		Map<String, Double> priors = new HashMap<String, Double>();			
		for(String vertex : graph.getVertices()) {
			priors.put(vertex, 1.0 / graph.getVertexCount());
		}

		return divrankScoring(graph, priors);
	}
	
	public static Map<String, Double> divrankScoring(Graph<String, Edge>  graph, final Map<String, Double> priors) {
		
		Map<Edge, Number> edgesMap = new HashMap<Edge, Number>();
		for(Edge edge : graph.getEdges()) {
			edgesMap.put(edge, edge.weight);
		}
		
		List<String> vertices = new ArrayList<String>(graph.getVertices());
		
		System.out.println("#priors: " + priors.size() + ", #vertices: " + vertices.size());
		
		double sum = 0;
		double[] initialScores = new double[vertices.size()];			
		
		int i = 0;
		for(String vertex : vertices) {
			initialScores[i] = priors.get(vertex);
			sum += initialScores[i];
			i++;
		}
		System.out.println("Initial SUM: " + sum);

		SparseDoubleMatrix2D matrix = GraphMatrixOperations.graphToSparseMatrix(graph, edgesMap);
		
		IntArrayList iIndinces = new IntArrayList();
		IntArrayList jIndinces = new IntArrayList();
		DoubleArrayList weights = new DoubleArrayList();
		
		matrix.getNonZeros(iIndinces, jIndinces, weights);
		
		SparseMatrix affinityMatrix = new SparseHashMatrix(vertices.size(), vertices.size());
		for(int index = 0; index < weights.size(); index++) {
			affinityMatrix.set(iIndinces.get(index), jIndinces.get(index), weights.get(index));
		}
		DoubleVector initialRanks = new DenseVector(initialScores);
		
		DivRank ranker = new DivRank(d);
		ranker.setIterations(20);
		
		DoubleVector ranks = ranker.rankMatrix(affinityMatrix, initialRanks);
		
		double maxScore = 0;
		Map<String, Double> verticesMap = new TreeMap<String, Double>();
		for(int index = 0 ; index<vertices.size(); index++) {
			
			String vertex = vertices.get(index);
			double score = ranks.get(index);
			if(score > maxScore) {
				maxScore = score;
			}
			
			verticesMap.put(vertex, score);
		}
	
		if(maxScore > 0) {
			for(Entry<String, Double> ve : verticesMap.entrySet()) {
				verticesMap.put(ve.getKey(), ve.getValue()/maxScore);
			}
		}
		
		sum = 0;
		for(Double weight : verticesMap.values()) {
			sum += weight;
		}
		System.out.println("Final SUM: " + sum);
		System.out.println("Max DR: " + Collections.max(verticesMap.values()));
		System.out.println("Min DR: " + Collections.min(verticesMap.values()));
		
		return verticesMap;
	}

	public static Map<String, Double> getPriors(Collection<String> ids, Map<String, Integer> popularities) {
		
		Map<String, Double> priors = new HashMap<String, Double>();
		Double popularitySum = 0d;
		for(String id : ids) {
			Integer popularity = popularities.get(id);
			if(popularity != null) {
				popularitySum += (popularity+1);
			}
		}
		
		for(String id : ids) {
			Integer popularity = popularities.get(id);
			if(popularity != null) {
				priors.put(id, (popularity.doubleValue() + 1)/popularitySum);
			}
			else {
				priors.put(id, .0);
			}
		}
		return priors;
	}
	
}
