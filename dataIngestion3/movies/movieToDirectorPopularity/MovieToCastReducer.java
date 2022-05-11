import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MovieToCastReducer extends Reducer<Text, Text, Text, NullWritable> {
	
	Text k = new Text();
	public double convert(String str){
	    int i = 0;
              double number = 0;
              boolean isNegative = false;
              int len = str.length();
              if( str.charAt(0) == '-' ){
                 isNegative = true;
                 i = 1;
              }
              while( i < len ){
                 int temp = str.charAt(i++) - '0';
                 if(temp>=0 && temp <= 9){
                 number *= 10.0;
                 number += temp;
                 }
              }
              if( isNegative ) {
                number = -number;
              }
              return number;
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String[] arr1 = key.toString().split("_");
		String dirId = arr1[0].trim();
		String year = arr1[1].trim();
		double totalProfit = 0, totalRevenue = 0, totalPopularity = 0;
		int size = 0;

		//try{
		//6_5400000_12400000@
		StringBuilder str = new StringBuilder();
		String ttt = "5400000";
		for(Text t : values) {
		    //str.append(t.toString()+"@");
			String[] arr2 = t.toString().split("_");
			//totalPopularity += Double.parseDouble(ttt);

			totalPopularity += convert(arr2[0].trim());
            totalProfit += convert(arr2[1].trim());
             totalRevenue += convert(arr2[2].trim());
// 			totalPopularity += Double.parseDouble(arr2[0].trim());
// 			totalProfit += Double.parseDouble(arr2[1].trim());
// 			totalRevenue += Double.parseDouble(arr2[2].trim());
			size++;
		}
		//k.set(str.toString());
		//}
// 		catch(NumberFormatException e)
//         {
//           return;
//         }
		String avgProfit = String.valueOf(totalProfit / size);
		String avgPopularity = String.valueOf(totalPopularity / size);
		String avgRevenue = String.valueOf(totalRevenue / size);
		k.set(dirId+","+year+","+avgProfit+","+avgPopularity+","+avgRevenue);
		context.write(k, NullWritable.get());
		
	}
}
 