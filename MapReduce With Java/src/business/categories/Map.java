package business.categories;

/**
 * Created by subhasis on 2/23/17.
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper class outputs records as (category name, 1) for those records that have Palo Alto.
 */
public class Map extends Mapper<LongWritable,Text, Text, NullWritable>{

    public static final String SEARCH_STRING = "Palo Alto";

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] values = value.toString().split("::");
        String address = values[1].toLowerCase();
        if(address.contains(SEARCH_STRING.toLowerCase())){
            String category_list= values[2];
            category_list = category_list.substring(5, category_list.length()-1);
            String[] categories = category_list.trim().split(",");
            for (String category : categories){
                context.write(new Text(category.trim()), NullWritable.get());
            }
        }

    }
}
