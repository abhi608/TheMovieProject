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

enum AwardYear 
{
	NULL,
	NOT_NULL
}

enum AwardCeremony 
{
	NULL,
	NOT_NULL
}

enum Outcome 
{
    NULL,
    NOT_NULL
}

public class CeremonyStatsMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	
	Set<String> DirectorNameSet = new HashSet<>();
	Set<String> YearSet = new HashSet<>();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.getCounter(Rows.ROW_COUNT).increment(1);
		
		String[] cols = value.toString().split("\t");
		
		if(Strings.isNullOrEmpty(cols[0])) 
        {
			context.getCounter(DirectorName.NULL).increment(1);
		} 
        else 
        {
			context.getCounter(DirectorName.NOT_NULL).increment(1);
			String dir_name = String.valueOf(cols[0]);
			if(DirectorNameSet.contains(dir_name)) 
            {
				context.getCounter(DirectorName.DUPLICATE).increment(1);
			} 
            else 
            {
				context.getCounter(DirectorName.UNIQUE).increment(1);
				DirectorNameSet.add(dir_name);
			}
		}
		
		
// Year
		if(Strings.isNullOrEmpty(cols[2])) 
		{
			context.getCounter(AwardYear.NULL).increment(1);
		} 
		else 
		{
			context.getCounter(AwardYear.NOT_NULL).increment(1);
		}
		
		
// Ceremony
		if(Strings.isNullOrEmpty(cols[1])) 
        {
			context.getCounter(AwardCeremony.NULL).increment(1);
		} 
        else 
        {
			context.getCounter(AwardCeremony.NOT_NULL).increment(1);
		}
		
		
//  Outcome
		if(Strings.isNullOrEmpty(cols[4])) 
        {
			context.getCounter(Outcome.NULL).increment(1);
		} 
        else 
        {
			context.getCounter(Outcome.NOT_NULL).increment(1);
		}
		
	}
	
}