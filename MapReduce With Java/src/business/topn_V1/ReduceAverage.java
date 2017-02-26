package business.topn;

/**
 * Created by subhasis on 2/23/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;


public class ReduceAverage extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
        double sum = 0.0;
        long count = 0;
        for(FloatWritable value : values){
            sum += value.get();
            count ++;
        }
        double average = sum/count;
        context.write(key, new FloatWritable((float)average));
    }
}
