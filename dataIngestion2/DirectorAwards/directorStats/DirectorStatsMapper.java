import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.google.common.base.Strings;

enum Rows 
{
	ROW_COUNT,
	ROWS_WITH_MISSING_COLUMNS
}

enum DirectorName 
{
	UNIQUE,
	DUPLICATE,
	NULL,
	NOT_NULL
}

enum tmdbID 
{
	UNIQUE,
	DUPLICATE,
	NULL,
	NOT_NULL
}

enum TotalAwards 
{
	NULL,
	NOT_NULL
}

public class DirectorStatsMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Set<String> directorNameSet = new HashSet<>();
	Set<String> tmdbIDSet = new HashSet<>();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {
		context.getCounter(Rows.ROW_COUNT).increment(1);
		String[] cols = value.toString().split("\t");
		
// Director Name
		if(Strings.isNullOrEmpty(cols[0])) 
        {
			context.getCounter(DirectorName.NULL).increment(1);
		} 
        else 
        {
			context.getCounter(DirectorName.NOT_NULL).increment(1);
			String dir_name = String.valueOf(cols[0]);
			if(directorNameSet.contains(dir_name)) 
            {
				context.getCounter(DirectorName.DUPLICATE).increment(1);
			} 
            else 
            {
				context.getCounter(DirectorName.UNIQUE).increment(1);
				directorNameSet.add(dir_name);
			}
		}
		
//tmdbID
		if(Strings.isNullOrEmpty(cols[1])) 
        {
			context.getCounter(tmdbID.NULL).increment(1);
		} 
        else 
        {
			context.getCounter(tmdbID.NOT_NULL).increment(1);
			String t_id = cols[1];
			if(tmdbIDSet.contains(t_id)) 
            {
				context.getCounter(tmdbID.DUPLICATE).increment(1);
			} 
            else 
            {
				context.getCounter(tmdbID.UNIQUE).increment(1);
				tmdbIDSet.add(t_id);
			}
		}
		
		
// Total Awards
		if(Strings.isNullOrEmpty(cols[3])) 
        {
			context.getCounter(TotalAwards.NULL).increment(1);
		} 
        else 
        {
			context.getCounter(TotalAwards.NOT_NULL).increment(1);
		}
		
	}
	
}