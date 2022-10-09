import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//MaxClosePriceReducer, to get the maximum value of each cryto current
public class MaxClosePriceReducer
  extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException {
    //the maximum value 
    double marketMax = Double.MIN_VALUE;
    for (DoubleWritable value : values) {
	    //iterate to get the max value
            marketMax = Math.max(marketMax, value.get());
      
    }
    //we print the max value to the output file
    context.write(key, new DoubleWritable(marketMax));
  }
}

