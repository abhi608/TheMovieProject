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

public class MovieToCastMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Text k = new Text();
	Text v = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] cols = value.toString().split(",");
		if(cols.length != 6) return;
		for(String s : cols) {
			if(Strings.isNullOrEmpty(s)) return;
		}
		String dirId = cols[0];
		String popularity = cols[2];
		String profit = cols[3];
		String year = cols[4];
		String revenue = cols[5];
		
		k.set(dirId + "_" + year);
		v.set(popularity+"_"+profit+"_"+revenue);
		
	}
	
}
