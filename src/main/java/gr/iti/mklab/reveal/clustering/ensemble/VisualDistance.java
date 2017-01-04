package gr.iti.mklab.reveal.clustering.ensemble;

import com.oculusinfo.ml.distance.DistanceFunction;
import com.oculusinfo.ml.feature.numeric.NumericVectorFeature;

/***
 * A distance function that computes the Euclidean distance between two VectorFeatures
 * 
 * @author slangevin
 *
 */
public class VisualDistance extends DistanceFunction<NumericVectorFeature> {
	private static final long serialVersionUID = -1493313434323633636L;

	public VisualDistance(double weight) {
		super(weight);
	}
	
	@Override
	public double distance(NumericVectorFeature x, NumericVectorFeature y) {
		double[] vector1 = x.getValue();
		double[] vector2 = y.getValue();
		
		double d = 0;
		for (int i = 0; i < vector1.length; i++) {
			d += Math.pow(vector1[i] - vector2[i], 2);
		}
		
		// return euclidean distance
		return Math.sqrt(d);
	}
}