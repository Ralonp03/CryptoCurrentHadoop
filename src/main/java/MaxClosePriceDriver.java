import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//clase MaxClosePriceDriver
public class MaxClosePriceDriver extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    //compare that the arguments are right
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    //create the job
    Job job = new Job(getConf(), "Max Close Price");
    job.setJarByClass(getClass());

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    //assign the mapper
    job.setMapperClass(MaxClosePriceMapper.class);
    //assign the combiner
    job.setCombinerClass(MaxClosePriceReducer.class);
    //asignamos el reducer
    job.setReducerClass(MaxClosePriceReducer.class);

    //specify the output of the key
    job.setOutputKeyClass(Text.class);
    //specify the output of the value
    job.setOutputValueClass(DoubleWritable.class);
    
    //we wait until the completation of the Mapreduce
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  //main
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxClosePriceDriver(), args);
    System.exit(exitCode);
  }
}

