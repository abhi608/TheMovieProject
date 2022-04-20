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

enum MovieId {
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

enum ReleaseDate {
	NULL,
	NOT_NULL
}

enum Genres {
	NULL,
	NOT_NULL
}

enum OriginalLanguage {
	NULL,
	NOT_NULL
}

enum SpokenLanguages {
	NULL,
	NOT_NULL
}

enum Budget {
	NULL,
	NOT_NULL
}

enum Revenue {
	NULL,
	NOT_NULL
}

enum Runtime {
	NULL,
	NOT_NULL
}

enum Popularity {
	NULL,
	NOT_NULL
}

enum VoteAvg {
	NULL,
	NOT_NULL
}

public class MovieStatsMapperWithCounter extends Mapper<LongWritable, Text, Text, Text> {
	
	Set<String> movieIdSet = new HashSet<>();
	Set<String> titleSet = new HashSet<>();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.getCounter(Rows.ROW_COUNT).increment(1);
		
		String[] cols = value.toString().split("\t");
		
//		At this point, total # columns = 11
		
//		movie_id
		if(cols[0] == null || cols[0].isBlank()) {
			context.getCounter(MovieId.NULL).increment(1);
		} else {
			context.getCounter(MovieId.NOT_NULL).increment(1);
			String movieId = String.valueOf(cols[0]);
			if(movieIdSet.contains(movieId)) {
				context.getCounter(MovieId.DUPLICATE).increment(1);
			} else {
				context.getCounter(MovieId.UNIQUE).increment(1);
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
		
		
//		releaseDate
		if(cols[2] == null || cols[2].isBlank()) {
			context.getCounter(ReleaseDate.NULL).increment(1);
		} else {
			context.getCounter(ReleaseDate.NOT_NULL).increment(1);
		}
		
		
//		genres
		if(cols[3] == null || cols[3].isBlank()) {
			context.getCounter(Genres.NULL).increment(1);
		} else {
			context.getCounter(Genres.NOT_NULL).increment(1);
		}
		
		
//		originalLanguage
		if(cols[4] == null || cols[4].isBlank()) {
			context.getCounter(OriginalLanguage.NULL).increment(1);
		} else {
			context.getCounter(OriginalLanguage.NOT_NULL).increment(1);
		}
		
		
//		spokenLanguages
		if(cols[5] == null || cols[5].isBlank()) {
			context.getCounter(SpokenLanguages.NULL).increment(1);
		} else {
			context.getCounter(SpokenLanguages.NOT_NULL).increment(1);
		}
		
		
//		budget
		if(cols[6] == null || cols[6].isBlank()) {
			context.getCounter(Budget.NULL).increment(1);
		} else {
			context.getCounter(Budget.NOT_NULL).increment(1);
		}
		
		
//		revenue
		if(cols[7] == null || cols[7].isBlank()) {
			context.getCounter(Revenue.NULL).increment(1);
		} else {
			context.getCounter(Revenue.NOT_NULL).increment(1);
		}
		
		
//		runtime
		if(cols[8] == null || cols[8].isBlank()) {
			context.getCounter(Runtime.NULL).increment(1);
		} else {
			context.getCounter(Runtime.NOT_NULL).increment(1);
		}
		
		
//		popularity
		if(cols[9] == null || cols[9].isBlank()) {
			context.getCounter(Popularity.NULL).increment(1);
		} else {
			context.getCounter(Popularity.NOT_NULL).increment(1);
		}
		
		
//		voteAvg
		if(cols[10] == null || cols[10].isBlank()) {
			context.getCounter(VoteAvg.NULL).increment(1);
		} else {
			context.getCounter(VoteAvg.NOT_NULL).increment(1);
		}

		
	}
	
}
