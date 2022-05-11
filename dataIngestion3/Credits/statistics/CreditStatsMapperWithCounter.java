import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.google.common.base.Strings;

enum Rows {
	ROW_COUNT,
	ROWS_WITH_MISSING_COLUMNS
}

enum Movie_id {
	UNIQUE,
	DUPLICATE,
	NULL,
	NOT_NULL
}

enum Cast {
	UNIQUE,
	DUPLICATE,
	NULL,
	NOT_NULL
}

enum Crew {
	UNIQUE,
	DUPLICATE,
	NULL,
	NOT_NULL
}

public class CreditStatsMapperWithCounter extends Mapper<LongWritable, Text, Text, Text> {
	
	Set<String> movieIdSet = new HashSet<>();
	Set<String> castSet = new HashSet<>();
	Set<String> crewSet = new HashSet<>();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.getCounter(Rows.ROW_COUNT).increment(1);
		
		String[] cols = value.toString().split("\t");
		if(cols.length != 3) {
			context.getCounter(Rows.ROWS_WITH_MISSING_COLUMNS).increment(1);
			return;
		}
		
//		movie_id
		if(Strings.isNullOrEmpty(cols[2])) {
			context.getCounter(Movie_id.NULL).increment(1);
		} else {
			context.getCounter(Movie_id.NOT_NULL).increment(1);
			String movieId = String.valueOf(cols[2]);
			if(movieIdSet.contains(movieId)) {
				context.getCounter(Movie_id.DUPLICATE).increment(1);
			} else {
				context.getCounter(Movie_id.UNIQUE).increment(1);
				movieIdSet.add(movieId);
			}
		}
		
		
//		cast
		if(Strings.isNullOrEmpty(cols[0])) {
			context.getCounter(Cast.NULL).increment(1);
		} else {
			context.getCounter(Cast.NOT_NULL).increment(1);
			String cast = cols[0];
			if(castSet.contains(cast)) {
				context.getCounter(Cast.DUPLICATE).increment(1);
			} else {
				context.getCounter(Cast.UNIQUE).increment(1);
				castSet.add(cast);
			}
		}
		
		
//		crew
		if(Strings.isNullOrEmpty(cols[1])) {
			context.getCounter(Crew.NULL).increment(1);
		} else {
			context.getCounter(Crew.NOT_NULL).increment(1);
			String crew = cols[1];
			if(crewSet.contains(crew)) {
				context.getCounter(Crew.DUPLICATE).increment(1);
			} else {
				context.getCounter(Crew.UNIQUE).increment(1);
				crewSet.add(crew);
			}
		}
		
	}
	
}
