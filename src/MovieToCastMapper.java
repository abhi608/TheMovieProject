import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.ParseException;

public class MovieToCastMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Text k = new Text();
	Text v = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] cols = value.toString().split("\t");
		if(cols.length != 4) return;
		String movieId = String.valueOf(cols[0]);
		k.set(movieId);
		String cast = cols[2];
		cast = cast.replaceAll("\"\\[", "[");
		cast = cast.replaceAll("\\]\"", "]");
		cast = cast.replaceAll("\"\"", "\"");
		try {
			JSONArray root = (JSONArray) JSONValue.parseWithException(cast);
//			Only consider the top 3 casts per movie.
//			Cast array is in decreasing order of importance
//			of casts in that movie
			for(int i=0; i<root.size() && i<3; i++) {
				JSONObject curCast = (JSONObject) root.get(i);
				String castId = String.valueOf((int) curCast.get("id"));
				v.set(castId);  
				context.write(k, v);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
}
