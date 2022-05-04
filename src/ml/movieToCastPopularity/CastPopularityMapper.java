import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.google.common.base.Strings;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.ParseException;

public class CastPopularityMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Text k = new Text();
	Text v = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] cols = value.toString().split("\t");
		if(cols.length != 5) return;
		for(String s : cols) {
			if(Strings.isNullOrEmpty(s)) return;
		}
		String[] castIds = cols[0].split(",");
		String voteAvg = cols[4].trim();
		v.set(voteAvg);
		for(String id : castIds) {
			k.set(id.trim());
			context.write(k, v);
		}
		
	}
	
}
