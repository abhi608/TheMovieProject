import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.NullWritable;

public class RemoveDuplicateRowsReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	Text k = new Text();
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		context.write(NullWritable.get(), values.iterator().next());
		
	}
}
 