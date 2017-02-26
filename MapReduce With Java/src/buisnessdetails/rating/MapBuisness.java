package buisnessdetails.rating;

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


public class MapBuisness extends Mapper<LongWritable,Text, Text, Text>{

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] values = value.toString().split("::");
        context.write(new Text(values[0]), new Text("B::"+values[1]+"\t"+values[2]));
    }
}
