package business.topn;

/**
 * Created by subhasis on 2/23/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


public class MapTopN extends Mapper<LongWritable,Text, NullWritable, Text>{

    private PriorityQueue<RatingModel> ratingMinHeap = new PriorityQueue<>();
    private static int topN = 0;

    @Override
    protected void setup(Context context)throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        topN = Integer.parseInt(conf.get("findTopN"));
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] values = value.toString().split("\t");
        String id = values[0];
        float rating = Float.parseFloat(values[1]);

        RatingModel topObj = ratingMinHeap.peek();
        if (ratingMinHeap.size() <= topN || rating > topObj.getRating()){
            ratingMinHeap.add(new RatingModel(id,rating));

            if(ratingMinHeap.size() > topN){
                ratingMinHeap.poll();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException{
        while(!ratingMinHeap.isEmpty()){
            context.write(NullWritable.get(),new Text(ratingMinHeap.poll().toString()));
        }
    }
}
