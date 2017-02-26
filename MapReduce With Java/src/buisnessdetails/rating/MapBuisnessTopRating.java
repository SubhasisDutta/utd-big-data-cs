package buisnessdetails.rating;

/**
 * Created by subhasis on 2/23/17.
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;



public class MapBuisnessTopRating extends Mapper<LongWritable,Text, Text, Text>{

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] values = value.toString().split("\t");
        String id = values[0];
        float rating = Float.parseFloat(values[1]);

        context.write(new Text(id), new Text("R::"+rating));

    }
}
