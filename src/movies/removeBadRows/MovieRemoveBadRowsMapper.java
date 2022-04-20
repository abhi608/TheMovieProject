package movies.removeBadRows;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.google.common.base.Strings;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.ParseException;

enum Errors {
	PARSEERROR,
	ROWS_WITH_MISSING_COLUMNS
}

public class MovieRemoveBadRowsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] cols = value.toString().split("\t");
		if(Strings.isNullOrEmpty(cols[2]) || Strings.isNullOrEmpty(cols[3]) 
				|| Strings.isNullOrEmpty(cols[5]) || Strings.isNullOrEmpty(cols[8])) {
			return;
		}
		
		context.write(value, NullWritable.get());
	}
	
}
