package business.topn;

/**
 * Created by subhasis on 2/23/17.
 */

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class MapAverage extends Mapper<LongWritable,Text, Text, FloatWritable>{

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] values = value.toString().split("::");
        String id = values[2];
        float rating = Float.parseFloat(values[3]);
        context.write(new Text(id), new FloatWritable(rating));
    }
}
