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


public class GetMoviesMetaDataMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	public int checkLeap(int y)
        {
           if ((y % 4 == 0 && y % 100 != 0) || (y % 400 == 0))
               return 1;
           else
               return 0;
        }
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String temp = value.toString();
		String day[] = { "Sunday",    "Monday",   "Tuesday",
                                 "Wednesday", "Thursday", "Friday",
                                 "Saturday" };
        int m_no[] = { 0, 3, 3, 6, 1, 4, 6, 2, 5, 0, 3, 5 };
		String[] moviesColArray = temp.split("\t");
		int i=0,n=0;
		StringBuilder sb = new StringBuilder();
		List<String> moviesGenreIdList = new ArrayList<>();
		List<String> moviesSpokenLanguageIdList = new ArrayList<>();

		if(moviesColArray.length != 24){
			return;
		}
		if(moviesColArray[0].equals("adult"))
			return;
		if(Strings.isNullOrEmpty(moviesColArray[2]) || Strings.isNullOrEmpty(moviesColArray[3]) || Strings.isNullOrEmpty(moviesColArray[5])||Strings.isNullOrEmpty(moviesColArray[6])||Strings.isNullOrEmpty(moviesColArray[7])||Strings.isNullOrEmpty(moviesColArray[10])||Strings.isNullOrEmpty(moviesColArray[14])||Strings.isNullOrEmpty(moviesColArray[15])||Strings.isNullOrEmpty(moviesColArray[16])||Strings.isNullOrEmpty(moviesColArray[17]) || Strings.isNullOrEmpty(moviesColArray[20])||Strings.isNullOrEmpty(moviesColArray[22])){
			return;
		}
		if(moviesColArray[2].equals("0") || moviesColArray[15].equals("0") || moviesColArray[16].equals("0"))
			return;

		try {
			String moviesBudget = moviesColArray[2];
			sb.append(moviesBudget+"\t");

			String genresArray = moviesColArray[3].replaceAll("\"\\[", "[").replaceAll("\\]\"", "]").replaceAll("'","\"");
			if(genresArray.equals("[]"))
				return;
			JSONArray genresJSONList = (JSONArray) JSONValue.parseWithException(genresArray);
			i=0;
			n = genresJSONList.size();
			while(i<n){
				JSONObject itrGenre = (JSONObject) genresJSONList.get(i);
				moviesGenreIdList.add(String.valueOf((int) itrGenre.get("id")));
				i++;
			}
			String moviesGenres = String.join(",", moviesGenreIdList);
			sb.append(moviesGenres+"\t");

			String id = moviesColArray[5];
			sb.append(id+"\t");

			String imdbID = moviesColArray[6];
			sb.append(imdbID+"\t");

			String movieOriginalLanguage = moviesColArray[7];
			sb.append(movieOriginalLanguage+"\t");

			String moviesPopularity = moviesColArray[10];
			sb.append(moviesPopularity+"\t");

			String moviesReleaseDate = moviesColArray[14];
			sb.append(moviesReleaseDate+"\t");

			String[] moviesDateArray = moviesReleaseDate.split("-");
			String year = moviesDateArray[0];
			sb.append(year+"\t");

			String month = moviesDateArray[1];
			sb.append(month+"\t");

			String date = moviesDateArray[2];
			sb.append(date+"\t");
			int yyyy = Integer.parseInt(year);
            int dd = Integer.parseInt(date);
            int mm = Integer.parseInt(month);
			int flag_for_leap = checkLeap(yyyy);
			int j=0;
            if ((yyyy / 100) % 2 == 0) {
                        if ((yyyy / 100) % 4 == 0)
                            j = 6;
                        else
                            j = 2;
                    }
                    else {
                        if (((yyyy / 100) - 1) % 4 == 0)
                            j = 4;
                        else
                            j = 0;
                    }
            String weekday = "";
            int weekt=0;
            int total = (yyyy % 100) + ((yyyy % 100) / 4) + dd
                                + m_no[mm - 1] + j;
                    if (flag_for_leap == 1) {
                        if ((total % 7) > 0)
                           weekt =(total % 7) - 1;
                        else
                            weekt = 6;
                    }
                    else
                       weekt = (total % 7);
            weekday = day[weekt];
            sb.append(weekday + "\t");

            String weekNum = Integer.toString(weekt);
            sb.append(weekNum + "\t");



			String moviesRevenue = moviesColArray[15];
			sb.append(moviesRevenue+"\t");

			long profit = Long.parseLong(moviesRevenue) - Long.parseLong(moviesBudget);
// 			if(profit< 0)
// 			    profit =0;
			String moviesProfit = Long.toString(profit);
			sb.append(moviesProfit + "\t");

			String runTime = moviesColArray[16];
			sb.append(runTime+"\t");

			String moviesSpokenLanguageArray = moviesColArray[17].replaceAll("\"\\[", "[").replaceAll("\\]\"", "]").replaceAll("'","\"");
			if(moviesSpokenLanguageArray.equals("[]"))
				return;
			JSONArray spokenLanguageJSONArray = (JSONArray) JSONValue.parseWithException(moviesSpokenLanguageArray);
			i=0;
			n = spokenLanguageJSONArray.size();
			while(i<n){
				JSONObject itr = (JSONObject) spokenLanguageJSONArray.get(i);
				moviesSpokenLanguageIdList.add((String) itr.get("iso_639_1"));
				i++;
			}
			String moviesSpokenLanguages = String.join(",", moviesSpokenLanguageIdList);
			sb.append(moviesSpokenLanguages+"\t");

			String moviesTitle = moviesColArray[20];
			sb.append(moviesTitle+"\t");

			String moviesVoteAvg = moviesColArray[22];
		    sb.append(moviesVoteAvg);

			String moviesKey = sb.toString();
			String[] tempArray = moviesKey.split("\t");

			if(tempArray.length != 18)
			    return;
			context.write(new Text(moviesKey), NullWritable.get());
			
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		
	}
	
}

//moviesMetaDataOutputWeekday
///user/ss14396/rproject/moviesMetaDataOutputWeekday/part-m-00000

///user/ss14396/rproject/moviesMetaDataOutputProfit/part-m-00000
