package business.topn;

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


public class ReduceTopN extends Reducer<NullWritable, Text, NullWritable, Text> {

    private PriorityQueue<RatingModel> ratingMinHeap = new PriorityQueue<RatingModel>();
    private static int topN = 0;
    private Stack<Text> topNStack = new Stack<>();

    @Override
    protected void setup(Context context)throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        topN = Integer.parseInt(conf.get("findTopN"));
        context.write(NullWritable.get(), new Text("Business ID \t Rating"));
    }

    @Override
    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        for(Text value: values){
            String line = value.toString();
            String[] data = line.split("\t");
            String id = data[0];
            float rating = Float.parseFloat(data[1]);

            RatingModel ratingObj = ratingMinHeap.peek();
            if(ratingMinHeap.size() <= topN || rating > ratingObj.getRating()){
                ratingMinHeap.add(new RatingModel(id,rating));

                if(ratingMinHeap.size() > topN){
                    ratingMinHeap.poll();
                }
            }
        }

        while(!ratingMinHeap.isEmpty()){
            topNStack.push(new Text(ratingMinHeap.poll().toString()));
        }
        while(!topNStack.isEmpty()){
            context.write(NullWritable.get(),topNStack.pop());
        }
    }
}
