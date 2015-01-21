package gr.iti.mklab.reveal.solr;

import gr.iti.mklab.simmo.items.Image;
import org.apache.solr.client.solrj.beans.Field;

/**
 * Created by kandreadou on 1/20/15.
 */
public class SolrImage {

    @Field(value = "id")
    public String id;

    @Field(value = "title")
    public String title;

    @Field(value = "description")
    public String description;

    @Field(value = "collection")
    public String collection;

    public SolrImage(){}

    public SolrImage(Image image, String collection){
        id = image.getObjectId().toString();
        title = image.getTitle();
        description = image.getDescription();
        this.collection = collection;
    }
}
