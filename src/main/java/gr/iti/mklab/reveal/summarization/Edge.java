package gr.iti.mklab.reveal.summarization;


import java.io.Serializable;

import org.apache.commons.collections15.Transformer;

public class Edge implements Serializable, Comparable<Edge> {
		
		/**
	 * 
	 */
	private static final long serialVersionUID = -5946499278437784918L;

		public Edge(double weight) {
			this.weight = weight;
		}
		
		public double getWeight() {
			return weight;
		}
		
		public double weight; 
		
		public static Transformer<Edge, Double> getEdgeTransformer() {
			return new Transformer<Edge, Double>() {
				@Override
				public Double transform(Edge edge) {
					return edge.weight;
				}	
			};
		}

		@Override
		public int compareTo(Edge that) {
		    if (this.weight <= that.weight) {
		    	return -1;
			}
		    else { 
		    	return 1;
		    }
		}
	}