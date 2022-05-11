import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class DirectorPopularityReducer extends Reducer<Text, Text, Text, Text> {
	
	Text v = new Text();
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		float voteAvg = 0f;

		for(Text vote : values) {
			voteAvg += Float.valueOf(vote.toString());
			count++;
		}
		if(count > 0) voteAvg /= count;

		v.set(String.valueOf(voteAvg));
		context.write(key, v);
		
	}
}
 