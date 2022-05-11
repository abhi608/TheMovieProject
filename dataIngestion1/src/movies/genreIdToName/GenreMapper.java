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


public class GenreMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Text k = new Text();
	Text v = new Text();
	
	private String clearStringFormatting(String str) {
		str = str.replaceAll("\"\\[", "[");
		str = str.replaceAll("\\]\"", "]");
		str = str.replaceAll("\"\"", "\"");
		return str;
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] cols = value.toString().split("\t");
		if(cols.length != 20) return;
		
		try {
			JSONArray genresList = (JSONArray) JSONValue.parseWithException(clearStringFormatting(cols[1]));
			for(int i=0; i<genresList.size(); i++) {
				JSONObject curGenre = (JSONObject) genresList.get(i);
				String genreId = String.valueOf((int) curGenre.get("id"));
				String genreName = (String) curGenre.getAsString("name");
				k.set(genreId);
				v.set(genreName);
				context.write(k, v);
			}
			
		} catch (ParseException e1) {
			e1.printStackTrace();
			
		}
		
	}
	
}
