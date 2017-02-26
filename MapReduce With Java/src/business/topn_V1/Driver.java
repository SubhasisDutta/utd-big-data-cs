package business.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by subhasis on 2/23/17.
 */
public class Driver extends Configured implements Tool {

    public static final int TOP_N = 2050;
    public static final String INTERMEDIATE_OUTPUT = "intermediate_output";

    @Override
    public int run(String[] args) throws Exception{

        Configuration conf = this.getConf();
        conf.set("findTopN",String.valueOf(TOP_N));

        FileSystem fs = FileSystem.get(conf);
        Path inputFilePath = new Path(args[0]);
        Path interMediateFilePath =  new Path(INTERMEDIATE_OUTPUT);
        Path outputFilePath = new Path(args[1]);
        if(fs.exists(interMediateFilePath)){
            fs.delete(interMediateFilePath,true);
        }
        if(fs.exists(outputFilePath)){
			/*If exist delete the output path*/
            fs.delete(outputFilePath,true);
        }

        /**
         * JOB 1: Find average rating of each buisness
         */
        Job job1 = Job.getInstance(conf);
        job1.setJobName("find-average-rating");
        job1.setJarByClass(Driver.class);

        job1.getConfiguration().set("mapreduce.output.textoutputformat.separator", "\t");
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);

        job1.setMapperClass(MapAverage.class);
        job1.setReducerClass(ReduceAverage.class);
        FileInputFormat.addInputPath(job1, inputFilePath);
        FileOutputFormat.setOutputPath(job1, interMediateFilePath);

        job1.waitForCompletion(true);

        /**
         * Job 2: Find Top N buisness
         */
        Job job2 = Job.getInstance(conf);
        job2.setJobName("find-top-n-buisness");
        job2.setJarByClass(Driver.class);

        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(MapTopN.class);
        job2.setReducerClass(ReduceTopN.class);
        FileInputFormat.addInputPath(job2, interMediateFilePath);
        FileOutputFormat.setOutputPath(job2, outputFilePath);

        return job2.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Driver(), args);
        System.exit(exitCode);
    }
}
