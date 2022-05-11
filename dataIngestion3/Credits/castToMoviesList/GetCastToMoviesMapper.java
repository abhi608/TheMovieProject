import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.ParseException;
import com.google.common.base.Strings;

public class GetCastToMoviesMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String result = value.toString();
		String[] colArray = result.split("\t");
		if(colArray.length != 3)
			return;
		if(Strings.isNullOrEmpty(colArray[0]))
			return;
		String movieID = colArray[2];
		String col_cast = colArray[0];
		if(col_cast.equals("cast"))
			return;
		col_cast = col_cast.replaceAll("\"\\[", "[");
		col_cast = col_cast.replaceAll("\\]\"", "]");
		col_cast = col_cast.replaceAll("'","\"");
		try{
			JSONArray jsonColCast = (JSONArray) JSONValue.parseWithException(col_cast);
			int itr=0;
			while(itr<jsonColCast.size() && itr < 3) {
				JSONObject itrCast = (JSONObject) jsonColCast.get(itr);
				String id = String.valueOf((int) itrCast.get("id"));
				context.write(new Text(id), new Text(movieID));
				itr++;
			}
		}catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
}
