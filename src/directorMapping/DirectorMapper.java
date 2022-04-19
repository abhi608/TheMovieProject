import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DirectorMapper extends Mapper<LongWritable, Text, Text, Text> {
	Text k = new Text();
	Text v = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		System.out.println("Started map");
		String[] cols = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
		k.set(key.toString());
		v.set(Arrays.toString(cols));
		context.write(k, v);
	}
	
}
