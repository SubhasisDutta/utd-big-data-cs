package user.rating;

/**
 * Created by subhasis on 2/23/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;


public class Map extends Mapper<LongWritable,Text, Text, Text>{

    private Set<String> filter_buisness_ids = new HashSet<>();
    private BufferedReader fis;

    @Override
    protected void setup(Context context)throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        try{
            Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            for (Path eachPath : localFiles) {
                //eachPath = new Path(conf.get("bs"));
                Path inFile = eachPath;
                if (fs.exists(inFile)) {
                    FSDataInputStream in = fs.open(inFile);
                    buildFilterSet(in,conf.get("filterCity").toLowerCase());
                    in.close();
                }
            }
        }
        catch(NullPointerException e)
        {
            System.out.println("Exception : "+e);
        }
    }

    private void buildFilterSet(FSDataInputStream business_file, String filter_city){
        try {
            fis = new BufferedReader(new InputStreamReader(business_file));
            String line = null;
            while ((line = fis.readLine()) != null) {
                String values[] = line.split("::");
                if(values[1].toLowerCase().contains(filter_city)){
                    filter_buisness_ids.add(values[0]);
                }
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.err.println("Exception while reading file'");
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] values = value.toString().split("::");
        String business_id = values[2];
        String user_id = values[1];
        String rating = values[3];
        if(filter_buisness_ids.contains(business_id)){
            context.write(new Text(user_id), new Text(rating));
        }
    }
}
