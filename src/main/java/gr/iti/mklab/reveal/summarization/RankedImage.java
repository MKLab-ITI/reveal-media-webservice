package gr.iti.mklab.reveal.summarization;

import org.mongodb.morphia.annotations.Id;

public class RankedImage implements Comparable<RankedImage> {
		
		public RankedImage(String id, double score) {
			this.id = id;
			this.score = score;
		}
		
		@Id
	    protected String id;
		
	    protected double score;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public double getScore() {
			return score;
		}

		public void setScore(double score) {
			this.score = score;
		}

		@Override
		public int compareTo(RankedImage other) {
			if(this.score == other.score) {
				return 0;
			}
			return this.score>other.score ? -1 : 1;
		}
		
	}