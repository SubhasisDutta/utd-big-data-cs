package business.categories;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class FindCategory extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception{

        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf);
        job.setJobName("findCategory");
        job.setJarByClass(FindCategory.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        Path inputFilePath = new Path(args[0]);
        Path outputFilePath = new Path(args[1]);
        if(fs.exists(outputFilePath)){
			/*If exist delete the output path*/
            fs.delete(outputFilePath,true);
        }

        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new FindCategory(), args);
        System.exit(exitCode);
    }
}
