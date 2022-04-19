import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CastToMovieReducer extends Reducer<Text, Text, Text, Text> {
	
	Text v = new Text();
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuilder str = new StringBuilder();
		String prefix = "";
		for(Text t : values) {
			str.append(prefix);
			str.append(t.toString());
			prefix = ",";
			
		}
		v.set(str.toString());
		context.write(key, v);
	}
}
 