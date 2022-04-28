package credits.movieToDirector;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MovieToDirectorReducer extends Reducer<Text, Text, Text, Text> {
	
	Text v = new Text();
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		context.write(key, values.iterator().next());
	}
}
 