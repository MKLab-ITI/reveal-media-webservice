package gr.iti.mklab.reveal.web;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashSet;
import java.util.Set;

/**
 * 
 * 	Created by kandreadou on 7/31/15.
 * 
 *  
 * 	{
 * 		"collection":"germanwings",
 * 		"isNew":true,
 * 		"keywords":["germanwings","crash"] 
 * 	} 
 * 
 * 	{
 * 		"collection":"paris",
 * 		"lon_min":2.282352,
 * 		"lat_min":48.837379,
 * 		"lon_max":2.394619,
 * 		"lat_max":48.891358 
 * 	} 
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CrawlPostRequest {

    private Set<String> keywords = new HashSet<>();

    private double lon_min, lat_min, lon_max, lat_max;

    private String collection;

    private boolean isNew;

    public CrawlPostRequest() {

    }

    public CrawlPostRequest(String collection, boolean isNew, Set<String> keywords, double lon_min, double lon_max, double lat_min, double lat_max) {
        this.collection = collection;
        this.isNew = isNew;
        this.keywords = keywords;
        this.lon_min = lon_min;
        this.lon_max = lon_max;
        this.lat_min = lat_min;
        this.lat_max = lat_max;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public Set<String> getKeywords() {
        return keywords;
    }

    public void setKeywords(Set<String> keywords) {
        this.keywords = keywords;
    }

    public double getLon_min() {
        return lon_min;
    }

    public void setLon_min(double lon_min) {
        this.lon_min = lon_min;
    }

    public double getLat_min() {
        return lat_min;
    }

    public void setLat_min(double lat_min) {
        this.lat_min = lat_min;
    }

    public double getLon_max() {
        return lon_max;
    }

    public void setLon_max(double lon_max) {
        this.lon_max = lon_max;
    }

    public double getLat_max() {
        return lat_max;
    }

    public void setLat_max(double lat_max) {
        this.lat_max = lat_max;
    }

    public boolean isNew() {
        return isNew;
    }

    public void setNew(boolean isNew) {
        this.isNew = isNew;
    }
}
