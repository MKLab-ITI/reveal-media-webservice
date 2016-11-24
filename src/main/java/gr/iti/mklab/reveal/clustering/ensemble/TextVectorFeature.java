package gr.iti.mklab.reveal.clustering.ensemble;

import com.oculusinfo.ml.feature.Feature;

import gr.iti.mklab.reveal.summarization.Vector;

public class TextVectorFeature extends Feature {

		/**
		 * 
		 */
		private static final long serialVersionUID = -6603866030456248145L;
		private Vector vector;
    	
		public TextVectorFeature(String name) {
			super(name);
		}
		
		public void setValue(Vector vector) {
			this.vector = vector;
		}

		public Vector getValue() {
			return this.vector;
		}
    }