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
import com.google.common.base.Strings;

enum Errors {
	PARSEERROR,
	ROWS_WITH_MISSING_COLUMNS
}

public class CeremonyColumnFilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	Text k = new Text();
//	Text v = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] cols = value.toString().split("\t");
		if(cols.length != 6) {
			context.getCounter(Errors.ROWS_WITH_MISSING_COLUMNS).increment(1);
			return;
		}
			String director_name = cols[0];

            String year = cols[2];

			String ceremony = cols[1];

			if(!Strings.isNullOrEmpty(ceremony) && ceremony.toLowerCase().contains("emmy")) {
				k.set(director_name + "\t" + year);
//				v.set(year);
				context.write(k, NullWritable.get());
			}
			
		
		
	}
	
}
