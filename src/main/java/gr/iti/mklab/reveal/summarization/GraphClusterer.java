package gr.iti.mklab.reveal.summarization;

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
	
    public static Collection<Collection<String>> cluster(Graph<String, Edge> graph, boolean singleItemClusters) {
    	
    	Set<String> clustered = new HashSet<String>();
    	Collection<Collection<String>> clusters = new ArrayList<Collection<String>>();
    	 
    	ScanCommunityDetector<String, Edge> detector = new ScanCommunityDetector<String, Edge>(scanEpsilon, scanMu);
		ScanCommunityStructure<String, Edge> structure = detector.getCommunityStructure(graph);
		
		int numCommunities = structure.getNumberOfCommunities();
        
		System.out.println("#communities " + numCommunities);
		System.out.println("#members " + structure.getNumberOfMembers());
		System.out.println("#hubs " + structure.getHubs().size());
		System.out.println("#outliers " + structure.getOutliers().size());
		
		for(Integer i=0; i<=numCommunities; i++) {
        	Community<String, Edge> community = structure.getCommunity(i);
              if(community != null) {
            	  Set<String> cluster = new HashSet<String>();
            	  cluster.addAll(community.getMembers());
            	  clustered.addAll(cluster);
            	  clusters.add(cluster);
              }
        }
        
        if(singleItemClusters) {
        	List<String> singleItems = new ArrayList<String>();
        	singleItems.addAll(structure.getHubs());
        	singleItems.addAll(structure.getOutliers());
        	for(String item : singleItems) {
        		Set<String> cluster = new HashSet<String>();
          	  	cluster.add(item);
          	  	clustered.addAll(cluster);
          	  	clusters.add(cluster);
        	}
        	
        	Set<String> unclustered = new HashSet<String>();
    		unclustered.addAll(graph.getVertices());
    		unclustered.removeAll(clustered);			
    		for(String item : unclustered) {
    			Set<String> cluster = new HashSet<String>();
          	  	cluster.add(item);
          	  	clusters.add(cluster);
    		}
        }

		return clusters;
	}	

}
