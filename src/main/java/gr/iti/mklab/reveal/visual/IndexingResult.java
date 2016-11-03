package gr.iti.mklab.reveal.visual;

import gr.iti.mklab.simmo.core.items.Media;

public class IndexingResult {

	public Media media;
	public double[] vector;
	
	public IndexingResult(Media media, double[] vector) {
		this.media = media;
		this.vector = vector;
	}
}
