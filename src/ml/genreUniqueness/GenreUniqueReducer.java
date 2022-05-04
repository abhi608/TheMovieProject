import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class GenreUniqueReducer extends Reducer<Text, Text, Text, Text> {
	
	Text v = new Text();
	int totalMovies = 4669;
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int size = 0;
		for(Text value : values) {
			size++;
		}
		float genreUniqueness = size / totalMovies;
		v.set(String.valueOf(genreUniqueness));
		context.write(key, v);
	}
}
 