package business.topn;

/**
 * Created by subhasis on 2/23/17.
 */

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MapTopN extends Mapper<LongWritable,Text, FloatWritable, Text>{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] values = value.toString().split("\t");
        String id = values[0];
        Float rating = Float.parseFloat(values[1]);

        context.write(new FloatWritable(rating), new Text(id));
    }
}
