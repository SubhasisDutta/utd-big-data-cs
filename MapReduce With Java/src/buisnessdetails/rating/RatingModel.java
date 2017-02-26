package buisnessdetails.rating;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;


/**
 * Created by subhasis on 2/24/17.
 */
public class RatingModel implements Comparable<RatingModel> {
    public ArrayList<String> getId() {
        return ids;
    }

    private ArrayList<String> ids;

    public Float getRating() {
        return rating.get();
    }

    private FloatWritable rating;

    public RatingModel(ArrayList<String> ids, float rating) {
        this.ids = ids;
        this.rating = new FloatWritable(rating);
    }

    @Override
    public int compareTo(RatingModel ratingObj){
        return this.rating.compareTo(ratingObj.rating);
    }

    @Override
    public String toString(){
        return this.ids+"\t"+this.rating;
    }

}
