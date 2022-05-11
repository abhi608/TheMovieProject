import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DirectorToIDDriver {
	
	public static void main(String... args) throws Exception {
		if (args.length != 2) {
			System.err.println("Please use the correct format as follows : {Input path} {Output path}");
			System.exit(-1);
		}
		String inputFile = args[0];
		Path inputPath = new Path(inputFile);
		String outputFile = args[1];
		Path outputPath = new Path(outputFile);

		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", "\t");

		FileSystem fileDir = FileSystem.get(conf);
		if (fileDir.exists(outputPath)) {
			fileDir.delete(outputPath, true);
		}

		Job jobDir = Job.getInstance(conf);
		jobDir.setJobName("Mapping the Director to MovieID");
		jobDir.setNumReduceTasks(1);
		jobDir.setJarByClass(DirectorToIDDriver.class);
		jobDir.setMapperClass(DirectorToIDMapper.class);
		jobDir.setReducerClass(DirectorToIDReducer.class);
		FileInputFormat.addInputPath(jobDir, inputPath);
		FileOutputFormat.setOutputPath(jobDir, outputPath);
		
		jobDir.setOutputKeyClass(Text.class);
		jobDir.setOutputValueClass(Text.class);
		
		System.exit(jobDir.waitForCompletion(true) ? 0 : 1);
		
	}	
	
}
