import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class GenreUniqueReducer extends Reducer<Text, Text, Text, Text> {
	
	Text v = new Text();
	double totalMovies = 4669d;
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int size = 0;
		for(Text value : values) {
			size++;
		}
		double genreUniqueness = size / totalMovies;
		genreUniqueness = -Math.log(genreUniqueness);
		v.set(String.valueOf(genreUniqueness));
		context.write(key, v);
	}
}
 