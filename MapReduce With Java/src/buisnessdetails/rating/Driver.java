package buisnessdetails.rating;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

    public static final int TOP_N = 10;
    public static final String INTERMEDIATE_OUTPUT_1 = "intermediate_output1";
    public static final String INTERMEDIATE_OUTPUT_2 = "intermediate_output2";

    @Override
    public int run(String[] args) throws Exception{

        Configuration conf = this.getConf();
        conf.set("findTopN",String.valueOf(TOP_N));

        FileSystem fs = FileSystem.get(conf);
        Path inputFilePath_review = new Path(args[0]);
        Path interMediateFilePath_avgRating =  new Path(INTERMEDIATE_OUTPUT_1);
        Path interMediateFilePath_topN =  new Path(INTERMEDIATE_OUTPUT_2);
        Path inputFilePath_business = new Path(args[1]);
        Path outputFilePath = new Path(args[2]);

        if(fs.exists(interMediateFilePath_avgRating)){
            fs.delete(interMediateFilePath_avgRating,true);
        }
        if(fs.exists(interMediateFilePath_topN)){
            fs.delete(interMediateFilePath_topN,true);
        }
        if(fs.exists(outputFilePath)){
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
        FileInputFormat.addInputPath(job1, inputFilePath_review);
        FileOutputFormat.setOutputPath(job1, interMediateFilePath_avgRating);

        job1.waitForCompletion(true);

        /**
         * Job 2: Find Top N buisness
         */
        Job job2 = Job.getInstance(conf);
        job2.setJobName("find-top-n-buisness");
        job2.setJarByClass(Driver.class);

        job2.setMapOutputKeyClass(FloatWritable.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(MapTopN.class);
        job2.setReducerClass(ReduceTopN.class);
        FileInputFormat.addInputPath(job2, interMediateFilePath_avgRating);
        FileOutputFormat.setOutputPath(job2, interMediateFilePath_topN);

        job2.waitForCompletion(true);

        /**
         * Job 3 : Find join of buisness and review using reduce side join.
         */
        Job job3 = Job.getInstance(conf);
        job3.setJobName("find-top-n-buisness-details");
        job3.setJarByClass(Driver.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.getConfiguration().set("mapreduce.output.textoutputformat.separator", "\t");

        job3.setReducerClass(ReduceTopBusinessDetails.class);
        MultipleInputs.addInputPath(job3, interMediateFilePath_topN,TextInputFormat.class, MapBuisnessTopRating.class );
        MultipleInputs.addInputPath(job3, inputFilePath_business,TextInputFormat.class, MapBuisness.class );

        FileOutputFormat.setOutputPath(job3, outputFilePath);

        return job3.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Driver(), args);
        System.exit(exitCode);
    }
}
