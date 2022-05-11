import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.ParseException;

enum Errors {
	PARSEERROR,
	ROWS_WITH_MISSING_COLUMNS
}

public class DirectorAwardsColumnFilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	Text k = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] cols = value.toString().split("\t");
		if(cols.length != 6676) {
			context.getCounter(Errors.ROWS_WITH_MISSING_COLUMNS).increment(1);
			return;
		}
			String director_name = cols[0];

            String tmdbID = cols[1];
			
			String total_awards = cols[3];
						
			StringBuilder sb = new StringBuilder();
			String keyToWrite = sb.append(director_name+"\t").append(tmdbID+"\t").append(total_awards+"\t").toString();
			k.set(keyToWrite);
			context.write(k, NullWritable.get());
		
	}
	
}