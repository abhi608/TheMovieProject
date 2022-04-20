import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RemoveDuplicateRowsReducer extends Reducer<Text, Text, Text, Text> {
	
	Text k = new Text();
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int minId = Integer.parseInt(values.iterator().next().toString());
		for(Text id : values) {
			int curId = Integer.parseInt(id.toString());
			minId = Math.min(minId, curId);
		}
		String movieId = String.valueOf(minId);
		k.set(movieId);
		context.write(k, key);
		
	}
}
 