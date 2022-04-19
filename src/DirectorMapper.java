import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.ParseException;


public class DirectorMapper extends Mapper<LongWritable, Text, Text, Text> {
	Text k = new Text();
	Text v = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] cols = value.toString().split("\t");
		if(cols.length != 4) return;
		String crew = cols[3];
//		crew = "{\"data\": " + crew + "}";
//		try {
//			JSONArray root = (JSONArray) JSONValue.parseWithException(crew);
//			for(int i=0; i<root.size(); i++) {
//				JSONObject curCrew = (JSONObject) root.get(i);
//				String job = (String) curCrew.get("job");
//				job = job.replaceAll("\\s+","");
//				if("director".equalsIgnoreCase(job)) {
//					String id = String.valueOf((int) curCrew.get("id"));
//					String name = (String) curCrew.get("name");
//					k.set(id);
//					v.set(name);
//					context.write(k, v);
//				}
//			}
//		} catch (ParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		
		k.set(Integer.toString(cols.length));
		v.set(cols[3]);
		context.write(k, v);
	}
	
}
