package buisnessdetails.rating;

/**
 * Created by subhasis on 2/23/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.Stack;


public class ReduceTopN extends Reducer<FloatWritable, Text, NullWritable, Text> {

    private PriorityQueue<RatingModel> ratingMinHeap = new PriorityQueue<RatingModel>();
    private static int topN = 0;
    private Stack<RatingModel> topNStack = new Stack<>();

    @Override
    protected void setup(Context context)throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        topN = Integer.parseInt(conf.get("findTopN"));
    }

    @Override
    public void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

        RatingModel ratingObj = ratingMinHeap.peek();
        Float rating = key.get();

        if(ratingMinHeap.size() <= topN || rating.compareTo(ratingObj.getRating()) > 0){
            ArrayList<String> ids = new ArrayList<>();
            for(Text value: values){
                ids.add(value.toString());
            }
            ratingMinHeap.add(new RatingModel(ids,rating));

            if(ratingMinHeap.size() > topN){
                ratingMinHeap.poll();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException{
        while(!ratingMinHeap.isEmpty()){
            topNStack.push(ratingMinHeap.poll());
        }
        while(!topNStack.isEmpty()){
            RatingModel record = topNStack.pop();

            ArrayList<String> ids = record.getId();

            for(String id: ids){
                context.write(NullWritable.get(),new Text(id+"\t"+record.getRating()));
            }
        }
    }
}
