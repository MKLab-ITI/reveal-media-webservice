package gr.iti.mklab.reveal.util;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Id;

/**
 * Created by kandreadou on 1/14/15.
 */
public class NamedEntity {

    @Id
    public ObjectId id;

    @Expose
    @SerializedName(value = "token")
    public String token;

    @Expose
    @SerializedName(value = "type")
    public String type;

    public int frequency;

    public NamedEntity() {
    }

    public NamedEntity(String token, String type, int frequency){
        this.token = token;
        this.type = type;
        this.frequency = frequency;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (!(o instanceof NamedEntity)) return false;
        return token.equalsIgnoreCase(((NamedEntity) o).token);
    }

}
