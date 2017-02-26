package user.rating;

/**
 * Created by subhasis on 2/23/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class Reduce extends Reducer<Text, Text, Text, Text> {

    private Set<String> filter_buisness_ids = new HashSet<>();
    private BufferedReader fis;

    @Override
    protected void setup(Context context)throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        context.write(new Text("User ID"), new Text("Rating"));
        try{
            Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());

            for (Path eachPath : localFiles) {
                //String fileName = eachPath.getName().toString().trim();
                eachPath = new Path(conf.get("bs"));
                buildFilterSet(eachPath,conf.get("filterCity").toLowerCase());
                /*String fileName = eachPath.getName().toString().trim();
                if (fileName.equals("business.csv"))
                {

                    break;
                }
                */
            }
        }
		catch(NullPointerException e)
        {
            System.out.println("Exception : "+e);
        }
    }

    private void buildFilterSet(Path business_file, String filter_city){
        try {

            fis = new BufferedReader(new FileReader(business_file.toString()));
            String line = null;
            while ((line = fis.readLine()) != null) {
                String values[] = line.split("::");
                if(values[1].toLowerCase().contains(filter_city)){
                    filter_buisness_ids.add(values[0]);
                }
            }
        } catch (IOException ioe) {
            System.err.println("Exception while reading stop word file '");
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        String buisness_id = key.toString();
        if(filter_buisness_ids.contains(buisness_id)){
            for(Text value : values){
                String[] t = value.toString().split("\t");
                context.write(new Text(t[0]), new Text(t[1]));
            }
        }
    }
}
