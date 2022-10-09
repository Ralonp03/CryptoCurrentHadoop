import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// The MaxClosePriceMapper class, that do the map function
public class MaxClosePriceMapper
    extends Mapper<LongWritable, Text, Text, DoubleWritable> {
  
  //create the parser to use it
  private MaxClosePriceParser parser = new MaxClosePriceParser();

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    //assing the parser the next value	
    parser.parse(value);
     //if the line, values... were wrong we discard those values
     if (parser.valorCorrecto()) {
      //we write key and value and give it to the reducers
      context.write(new Text(parser.getMonedaName()),
          new DoubleWritable(parser.getValue()));
    }
  }
}

