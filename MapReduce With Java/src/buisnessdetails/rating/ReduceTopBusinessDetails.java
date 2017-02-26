package buisnessdetails.rating;

/**
 * Created by subhasis on 2/23/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Stack;


public class ReduceTopBusinessDetails extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void setup(Context context)throws IOException, InterruptedException{
        context.write(new Text("Business ID"), new Text(" Full Address \t Categories \t Rating"));
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

        boolean has_top_rating = false;
        boolean has_buisness_details = false;

        String details = null;
        String rating = null;

        for(Text value: values){
            if(value.toString().charAt(0)=='R'){
                has_top_rating = true;
                rating = value.toString().substring(3);
            }
            else if(value.toString().charAt(0)=='B'){
                has_buisness_details = true;
                details = value.toString().substring(3);
            }
        }
        if(has_top_rating && has_buisness_details){
            context.write(key,new Text(details+"\t"+rating));
        }
    }
}
