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

enum Errors {
	PARSEERROR,
	COLUMNS_MISSING
}

public class MovieFilterColumnMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	Text k = new Text();
	
	private String clearStringFormatting(String str) {
		str = str.replaceAll("\"\\[", "[");
		str = str.replaceAll("\\]\"", "]");
		str = str.replaceAll("\"\"", "\"");
		return str;
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] cols = value.toString().split("\t");
		if(cols.length != 20) {
			context.getCounter(Errors.COLUMNS_MISSING).increment(1);
			return;
		}
		try {
			String budget = cols[0];
			
			JSONArray genresList = (JSONArray) JSONValue.parseWithException(clearStringFormatting(cols[1]));
			List<String> genreId = new ArrayList<>();
			for(int i=0; i<genresList.size(); i++) {
				JSONObject curGenre = (JSONObject) genresList.get(i);
				genreId.add(String.valueOf((int) curGenre.get("id")));
			}
			String genres = String.join(",", genreId);
			
			String movieId = cols[3];
			
			String originalLanguage = cols[5];
			
			String popularity = cols[8];
			
			String releaseDate = cols[11];
			
			String revenue = cols[12];
			
			String runTime = cols[13];
			
			JSONArray spokenLangList = (JSONArray) JSONValue.parseWithException(clearStringFormatting(cols[14]));
			List<String> spokenLangId = new ArrayList<>();
			for(int i=0; i<spokenLangList.size(); i++) {
				JSONObject curLang = (JSONObject) spokenLangList.get(i);
				spokenLangId.add((String) curLang.get("iso_639_1"));
			}
			String spokenLanguages = String.join(",", spokenLangId);
			
			String title = cols[17];
			
			String voteAvg = cols[18];
			
			StringBuilder sb = new StringBuilder();
			String keyToWrite = sb.append(movieId+"\t").append(title+"\t").append(releaseDate+"\t").append(genres+"\t").append(originalLanguage+"\t").append(spokenLanguages+"\t").append(budget+"\t").append(revenue+"\t").append(runTime+"\t").append(popularity+"\t").append(voteAvg).toString();
			k.set(keyToWrite);
			context.write(k, NullWritable.get());
			
		} catch (ParseException e1) {
			context.getCounter(Errors.PARSEERROR).increment(1);
			e1.printStackTrace();
			
		}
		
	}
	
}
