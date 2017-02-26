package business.categories;

/**
 * Created by subhasis on 2/23/17.
 */

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer returns the unique keys.
 */
public class Reduce extends Reducer<Text, NullWritable, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{
        if(key.getLength() > 0) {
            context.write(key,null);
        }
    }
}
