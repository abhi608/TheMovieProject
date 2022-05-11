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
	    if(value.toString().contains("Popularity"))
	        return;
		String[] cols = value.toString().split("\t");
		if(cols.length != 16) return;
		if(cols[10].equals("Popularity"))
		    return;

		for(String s : cols) {
			if(Strings.isNullOrEmpty(s)) return;
		}
		String dirId = cols[1];
		String popularity = cols[10];
		String profit = cols[11];
		String year = cols[8];
		String revenue = cols[12];
		
		k.set(dirId + "_" + year);
		v.set(popularity+"_"+profit+"_"+revenue);
		context.write(k, v);
		
	}
	
}


///user/ss14396/directorPopularityOutput/part-r-00000
