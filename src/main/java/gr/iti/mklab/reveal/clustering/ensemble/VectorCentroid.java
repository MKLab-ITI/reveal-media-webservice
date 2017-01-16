package gr.iti.mklab.reveal.clustering.ensemble;

import java.util.Collection;
import java.util.Collections;

import com.oculusinfo.ml.centroid.Centroid;

import gr.iti.mklab.reveal.summarization.Vector;

public class VectorCentroid implements Centroid<TextVectorFeature> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3889074315061705689L;
	
	private Vector fv;
	private String name;

	public VectorCentroid() {
		super();
	}
	
	@Override
	public void add(TextVectorFeature tfv) {
		if(fv == null) {
			fv = new Vector();
		}
		
		fv.mergeVector(tfv.getValue());
	}

	@Override
	public Collection<TextVectorFeature> getAggregatableCentroid() {
		return Collections.singleton(getCentroid());
	}

	@Override
	public TextVectorFeature getCentroid() {
		TextVectorFeature tfv = new TextVectorFeature(name);
		fv.updateLength();
		
		tfv.setValue(fv);
		return tfv;
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public Class<TextVectorFeature> getType() {
		return TextVectorFeature.class;
	}

	@Override
	public void remove(TextVectorFeature tfv) {
		fv.subtrackVector(tfv.getValue());
	}

	@Override
	public void reset() {
		fv.reset();
	}

	@Override
	public void setName(String name) {
		this.name = name;
	}
	
}