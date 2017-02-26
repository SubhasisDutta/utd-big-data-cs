package user.rating;

/**
 * Created by subhasis on 2/23/17.
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class Map extends Mapper<LongWritable,Text, Text, Text>{

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] values = value.toString().split("::");
        String business_id = values[2];
        String user_id = values[1];
        String rating = values[3];

        context.write(new Text(business_id), new Text(user_id+"\t"+rating));
    }
}
