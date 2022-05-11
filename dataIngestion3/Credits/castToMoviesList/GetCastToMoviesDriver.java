import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GetCastToMoviesDriver {
	
	public static void main(String... args) throws Exception {
		if (args.length != 2) {
			System.err.println("Use the correct format as: [Input path] [Output path]");
			System.exit(-1);
		}
		String inputFile = args[0];
		Path inputPath = new Path(inputFile);
		String outputFile = args[1];
		Path outputPath = new Path(outputFile);

		Configuration castConf = new Configuration();
		castConf.set("mapreduce.output.textoutputformat.separator", "\t");
		FileSystem castFs = FileSystem.get(castConf);
		if (castFs.exists(outputPath)) {
			castFs.delete(outputPath, true);
		}
		Job castJob = Job.getInstance(castConf);
		castJob.setJobName("Construction of Cast to Movies ID mapping");
		castJob.setNumReduceTasks(1);
		castJob.setJarByClass(GetCastToMoviesDriver.class);
		castJob.setMapperClass(GetCastToMoviesMapper.class);
		castJob.setReducerClass(GetCastToMoviesReducer.class);
		FileInputFormat.addInputPath(castJob, inputPath);
		FileOutputFormat.setOutputPath(castJob, outputPath);
		castJob.setOutputKeyClass(Text.class);
		castJob.setOutputValueClass(Text.class);
		System.exit(castJob.waitForCompletion(true) ? 0 : 1);
	}
	
}
