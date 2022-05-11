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
import com.google.common.base.Strings;


public class DirectorToIDMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String result = value.toString();
		String[] colArray = result.split("\t");
		if(colArray.length != 3)
			return;
		if(Strings.isNullOrEmpty(colArray[1]))
			return;
		String col_dir = colArray[1];
		if(col_dir.equals("crew"))
			return;
		col_dir = col_dir.replaceAll("\"\\[", "[").replaceAll("\\]\"", "]").replaceAll("'","\"");
		String crew = "director";
		try{
			JSONArray jsonColCast = (JSONArray) JSONValue.parseWithException(col_dir);
			int itr=0;
			while(itr<jsonColCast.size()) {
				JSONObject itrCast = (JSONObject) jsonColCast.get(itr);
				String crewJob = ((String) itrCast.get("job"));
				crewJob = crewJob.trim();
				crewJob = crewJob.toLowerCase();
				if(crew.equals(crewJob)) {
					String dirName = (String) itrCast.get("name");
					String dirId = String.valueOf((int) itrCast.get("id"));
					context.write(new Text(dirId), new Text(dirName));
				}
				itr++;
			}
		}catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
}
