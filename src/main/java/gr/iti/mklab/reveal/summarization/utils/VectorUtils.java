package gr.iti.mklab.reveal.summarization.utils;

import java.util.Arrays;
import java.util.Random;

public class VectorUtils {

	private static Random r = new Random();
	
    public static Double[] getRandomVector(int n) {
    	Double[] v = new Double[n];
		Arrays.fill(v, .0);
		for(int i = 0; i<n; i++) {
			v[i] = r.nextDouble();
		}
		
		normalize(v);
		return v;
    }
    
    public static void normalize(Double[] v) {
    	double sum = .0;
    	for(int i = 0; i<v.length; i++) {
			sum = v[i] * v[i];
		}
    	sum = Math.sqrt(sum);
    	
    	for(int i = 0; i<v.length; i++) {
			v[i] = v[i]/sum;
		}
    }
    
    public static void normalize(double[] v) {
    	double sum = .0;
    	for(int i = 0; i<v.length; i++) {
			sum = v[i] * v[i];
		}
    	sum = Math.sqrt(sum);
    	
    	for(int i = 0; i<v.length; i++) {
			v[i] = v[i]/sum;
		}
    }
}
