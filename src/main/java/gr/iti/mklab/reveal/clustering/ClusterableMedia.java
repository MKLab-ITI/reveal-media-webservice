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
public class ClusterableMedia implements Clusterable {

    public double[] vector;
    public Media item;

    public ClusterableMedia(Media item, double[] vector) {
        this.item = item;
        this.vector = vector;
    }

    @Override
    public double[] getPoint() {
        return vector;
    }
}
