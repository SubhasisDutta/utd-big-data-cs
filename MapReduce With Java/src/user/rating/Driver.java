package user.rating;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by subhasis on 2/23/17.
 */
public class Driver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception{

        Configuration conf = this.getConf();

        FileSystem fs = FileSystem.get(conf);
        Path inputFilePath_review = new Path(args[0]);
        Path inputFilePath_business = new Path(args[1]);
        conf.set("bs",args[1]);
        conf.set("filterCity",args[2]);
        Path outputFilePath = new Path(args[3]);

        if(fs.exists(outputFilePath)){
            fs.delete(outputFilePath,true);
        }
        Job job1 = Job.getInstance(conf);
        /**
         * JOB 1: Filter out buisnessId(key), user ID and rating in map
         * in reducer check if buisness ID in 'Stanford' then
         */

        job1.setJobName("find-user-rating");
        job1.setJarByClass(Driver.class);

        job1.getConfiguration().set("mapreduce.output.textoutputformat.separator", "\t");
        /**
         *  Load the Buisness.csv file into Distributed cache
         */

        DistributedCache.addCacheFile(inputFilePath_business.toUri(), job1.getConfiguration());

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setMapperClass(Map.class);
        job1.setReducerClass(Reduce.class);
        FileInputFormat.addInputPath(job1, inputFilePath_review);
        FileOutputFormat.setOutputPath(job1, outputFilePath);

        return job1.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Driver(), args);
        System.exit(exitCode);
    }
}
