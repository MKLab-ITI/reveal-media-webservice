package gr.iti.mklab.reveal.clustering;

import gr.iti.mklab.simmo.core.items.Media;
import org.apache.commons.math3.ml.clustering.Clusterable;

/**
 * A wrapper class around an Image or Video and its feature vector,
 * which implements the Clusterable interface in order to get clustered
 * by a Clusterer
 *
 * @author kandreadou
 *
 */
public class VisualClusterable implements Clusterable {

	private double[] vector;
    private String id;

    public VisualClusterable(Media item, double[] vector) {
        this.id = item.getId();
    	this.vector = vector;
    }

    public String getId() {
        return id;
    }
    
    @Override
    public double[] getPoint() {
        return vector;
    }
}
