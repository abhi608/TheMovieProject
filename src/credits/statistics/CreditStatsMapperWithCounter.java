import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

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

enum Title {
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
	Set<String> titleSet = new HashSet<>();
	Set<String> castSet = new HashSet<>();
	Set<String> crewSet = new HashSet<>();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.getCounter(Rows.ROW_COUNT).increment(1);
		
		String[] cols = value.toString().split("\t");
		if(cols.length != 4) {
			context.getCounter(Rows.ROWS_WITH_MISSING_COLUMNS).increment(1);
			return;
		}
		
//		movie_id
		if(cols[0] == null || cols[0].isBlank()) {
			context.getCounter(Movie_id.NULL).increment(1);
		} else {
			context.getCounter(Movie_id.NOT_NULL).increment(1);
			String movieId = String.valueOf(cols[0]);
			if(movieIdSet.contains(movieId)) {
				context.getCounter(Movie_id.DUPLICATE).increment(1);
			} else {
				context.getCounter(Movie_id.UNIQUE).increment(1);
				movieIdSet.add(movieId);
			}
		}
		
		
//		title
		if(cols[1] == null || cols[1].isBlank()) {
			context.getCounter(Title.NULL).increment(1);
		} else {
			context.getCounter(Title.NOT_NULL).increment(1);
			String title = cols[1];
			if(titleSet.contains(title)) {
				context.getCounter(Title.DUPLICATE).increment(1);
			} else {
				context.getCounter(Title.UNIQUE).increment(1);
				titleSet.add(title);
			}
		}
		
		
//		cast
		if(cols[2] == null || cols[2].isBlank()) {
			context.getCounter(Cast.NULL).increment(1);
		} else {
			context.getCounter(Cast.NOT_NULL).increment(1);
			String cast = cols[2];
			if(castSet.contains(cast)) {
				context.getCounter(Cast.DUPLICATE).increment(1);
			} else {
				context.getCounter(Cast.UNIQUE).increment(1);
				castSet.add(cast);
			}
		}
		
		
//		crew
		if(cols[3] == null || cols[3].isBlank()) {
			context.getCounter(Crew.NULL).increment(1);
		} else {
			context.getCounter(Crew.NOT_NULL).increment(1);
			String crew = cols[3];
			if(crewSet.contains(crew)) {
				context.getCounter(Crew.DUPLICATE).increment(1);
			} else {
				context.getCounter(Crew.UNIQUE).increment(1);
				crewSet.add(crew);
			}
		}
		
	}
	
}
