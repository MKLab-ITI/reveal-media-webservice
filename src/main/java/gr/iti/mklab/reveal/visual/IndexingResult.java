package gr.iti.mklab.reveal.visual;

import gr.iti.mklab.simmo.core.items.Media;

public class MediaCallableResult {

	public Media media;
	public double[] vector;
	
	public MediaCallableResult(Media media, double[] vector){
		this.media = media;
		this.vector = vector;
	}
}
