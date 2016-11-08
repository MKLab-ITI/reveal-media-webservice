package gr.iti.mklab.reveal.summarization;

import org.apache.commons.lang3.tuple.Pair;

import gr.iti.mklab.reveal.summarization.utils.L2;

public class Vertex {

	private Double[] visualVector;
	private Vector textualvector;
	
    private String id;

    public Vertex(String id, Double[] doubles, Vector textualvector) {
        this.id = id;
    	this.setVisualVector(doubles);
    	this.setTextualvector(textualvector);
    }

    public String getId() {
        return id;
    }

	public Vector getTextualvector() {
		return textualvector;
	}

	public void setTextualvector(Vector textualvector) {
		this.textualvector = textualvector;
	}

	public Double[] getVisualVector() {
		return visualVector;
	}

	public void setVisualVector(Double[] visualVector) {
		this.visualVector = visualVector;
	}

	public Pair<Double, Double> similarities(Vertex other) {
		Double vsim = (this.visualVector == null || other.visualVector == null) ? .0 : L2.similarity(this.visualVector, other.visualVector);
		Double tsim = (this.textualvector == null || other.textualvector == null) ? .0 : this.textualvector.cosine(other.textualvector);
		
		return Pair.of(tsim, vsim);
	}
	
}
