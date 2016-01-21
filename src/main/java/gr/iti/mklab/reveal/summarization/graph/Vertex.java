package gr.iti.mklab.reveal.summarization.graph;

public class Vertex {
	
	private String id;
	private double weight;

	public Vertex(String id, double weight) {
		this.setId(id);
		this.setWeight(weight);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public double getWeight() {
		return weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}
	
	public String toString() {
		return id;
	}
	
}
