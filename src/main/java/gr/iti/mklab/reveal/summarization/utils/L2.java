package gr.iti.mklab.reveal.summarization.utils;

public class L2 {

    public static double similarity(double[] v1, double[] v2) {
        return 1 - distance(v1, v2)/Math.sqrt(2);
    }

    public static Double similarity(Double[] v1, Double[] v2) {
        return 1. - distance(v1, v2)/Math.sqrt(2);
    }
    
    public static double distance(double[] v1, double[] v2) {
    	if(v1.length != v2.length) {
    		return -1;
    	}
    	
    	double distance = 0;
        for (int i = 0; i < v1.length; i++) {	
            distance += Math.pow((v1[i]-v2[i]), 2);
        }
          
        return Math.sqrt(distance);
    }
    
    public static Double distance(Double[] v1, Double[] v2) {
    	if(v1.length != v2.length) {
    		return -1.;
    	}
    	
    	Double distance = 0.;
        for (int i = 0; i < v1.length; i++) {	
            distance += Math.pow((v1[i]-v2[i]), 2);
        }
          
        return Math.sqrt(distance);
    }
    
}