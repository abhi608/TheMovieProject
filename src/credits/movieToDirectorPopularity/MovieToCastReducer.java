import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MovieToCastReducer extends Reducer<Text, Text, Text, NullWritable> {
	
	Text k = new Text();
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String[] arr1 = key.toString().split("_");
		String dirId = arr1[0];
		String year = arr1[1];
		double totalProfit = 0, totalRevenue = 0, totalPopularity = 0;
		int size = 0;
		for(Text t : values) {
			String[] arr2 = t.toString().split("_");
			totalPopularity += Double.valueOf(arr2[0]);
			totalProfit += Double.valueOf(arr2[1]);
			totalRevenue += Double.valueOf(arr2[2]);
			size++;
		}
		String avgProfit = String.valueOf(totalProfit / size);
		String avgPopularity = String.valueOf(totalPopularity / size);
		String avgRevenue = String.valueOf(totalRevenue / size);
		k.set(dirId+","+year+","+avgProfit+","+avgPopularity+","+avgRevenue);
		context.write(k, NullWritable.get());
		
	}
}
 