package gr.iti.mklab.reveal.summarization.graph;

import edu.uci.ics.jung.graph.Graph;
import gr.iti.mklab.reveal.summarization.scan.Community;
import gr.iti.mklab.reveal.summarization.scan.ScanCommunityDetector;
import gr.iti.mklab.reveal.summarization.scan.ScanCommunityStructure;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;



public class GraphClusterer {

	public static double scanEpsilon = 0.5;
	public static int scanMu = 3;
	
    public static <K> Collection<Collection<K>> cluster(Graph<K, Edge> graph, boolean singleItemClusters) {
    	
    	Set<K> clustered = new HashSet<K>();
    	Collection<Collection<K>> clusters = new ArrayList<Collection<K>>();
    	 
    	ScanCommunityDetector<K, Edge> detector = new ScanCommunityDetector<K, Edge>(scanEpsilon, scanMu);
		ScanCommunityStructure<K, Edge> structure = detector.getCommunityStructure(graph);
		
		int numCommunities = structure.getNumberOfCommunities();
        
		System.out.println("#communities " + numCommunities);
		System.out.println("#members " + structure.getNumberOfMembers());
		System.out.println("#hubs " + structure.getHubs().size());
		System.out.println("#outliers " + structure.getOutliers().size());
		
		for(Integer i=0; i<=numCommunities; i++) {
        	Community<K, Edge> community = structure.getCommunity(i);
              if(community != null) {
            	  Set<K> cluster = new HashSet<K>();
            	  cluster.addAll(community.getMembers());
            	  clustered.addAll(cluster);
            	  clusters.add(cluster);
              }
        }
        
        if(singleItemClusters) {
        	List<K> singleItems = new ArrayList<K>();
        	singleItems.addAll(structure.getHubs());
        	singleItems.addAll(structure.getOutliers());
        	for(K item : singleItems) {
        		Set<K> cluster = new HashSet<K>();
          	  	cluster.add(item);
          	  	clustered.addAll(cluster);
          	  	clusters.add(cluster);
        	}
        	
        	Set<K> unclustered = new HashSet<K>();
    		unclustered.addAll(graph.getVertices());
    		unclustered.removeAll(clustered);			
    		for(K item : unclustered) {
    			Set<K> cluster = new HashSet<K>();
          	  	cluster.add(item);
          	  	clusters.add(cluster);
    		}
        }

		return clusters;
	}	

    
}
