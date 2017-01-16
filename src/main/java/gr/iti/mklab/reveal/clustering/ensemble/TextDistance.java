package gr.iti.mklab.reveal.clustering.ensemble;

import com.oculusinfo.ml.distance.DistanceFunction;

import gr.iti.mklab.reveal.summarization.Vector;

public class TextDistance extends DistanceFunction<TextVectorFeature> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1044520279258863015L;

	public TextDistance(double weight) {
		super(weight);
	}
		
	@Override
	public double distance(TextVectorFeature tvf1, TextVectorFeature tvf2) {
		
		Vector v1 = tvf1.getValue();
		Vector v2 = tvf2.getValue();
		
		double similarity = Math.min(1, v1.cosine(v2));
		
		double d = 1. - similarity;
		
		if(d != 0 && (v1.getTerms().size() < 3 || v2.getTerms().size() < 3)) {
			return 1.;
		}
		
		return d;
	}	
}