import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class GetMoviesToCastListReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int itr =0;
		StringBuilder out = new StringBuilder();
		while(values.iterator().hasNext()){
			Text tt = values.iterator().next();
			String temp = tt.toString();
			if(itr==0)
				out.append(temp);
			else
				out.append(","+temp);
			itr++;
		}
		context.write(key, new Text(out.toString()));
	}
}
 