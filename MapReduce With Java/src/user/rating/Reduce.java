package user.rating;

/**
 * Created by subhasis on 2/23/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;



public class Reduce extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void setup(Context context)throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        context.write(new Text("User ID"), new Text("Rating"));
    }
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        String user_id = key.toString();
        for(Text value : values){
            context.write(new Text(user_id), value);
        }
    }
}
