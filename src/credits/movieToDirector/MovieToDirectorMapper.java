import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.ParseException;

public class MovieToDirectorMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Text k = new Text();
	Text v = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] cols = value.toString().split("\t");
		if(cols.length != 4) return;
		String movieId = String.valueOf(cols[0]);
		k.set(movieId);
		String crew = cols[3];
		crew = crew.replaceAll("\"\\[", "[");
		crew = crew.replaceAll("\\]\"", "]");
		crew = crew.replaceAll("\"\"", "\"");
		try {
			JSONArray root = (JSONArray) JSONValue.parseWithException(crew);
//			Only consider the top director per movie.
//			Crew array is in decreasing order of importance
//			of crew in that movie
			for(int i=0; i<root.size(); i++) {
				JSONObject curCrew = (JSONObject) root.get(i);
				String job = (String) curCrew.get("job");
				job = job.replaceAll("\\s+","");
				if("director".equalsIgnoreCase(job)) {
					String id = String.valueOf((int) curCrew.get("id"));
					v.set(id);
					context.write(k, v);
					break;
				}
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
}
