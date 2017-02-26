package business.topn;

import org.apache.hadoop.io.FloatWritable;


/**
 * Created by subhasis on 2/24/17.
 */
public class RatingModel implements Comparable<RatingModel> {
    public String getId() {
        return id;
    }

    private String id;

    public float getRating() {
        return Float.parseFloat(rating.toString());
    }

    private FloatWritable rating;

    public RatingModel(String id, float rating) {
        this.id = id;
        this.rating = new FloatWritable(rating);
    }

    @Override
    public int compareTo(RatingModel ratingObj){
        return this.rating.compareTo(ratingObj.rating);
    }

    @Override
    public String toString(){
        return this.id+"\t"+this.rating;
    }

}
