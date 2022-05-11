import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GetMoviesToCastListDriver {
	
	public static void main(String... args) throws Exception {
		if (args.length != 2) {
			System.err.println("The usage is wrong. The correct format is [Input path] [Output path]");
			System.exit(-1);
		}
		String inputFile = args[0];
		Path inputPath = new Path(inputFile);
		String outputFile = args[1];
		Path outputPath = new Path(outputFile);
		Configuration moviesConf = new Configuration();
		moviesConf.set("mapreduce.output.textoutputformat.separator", "\t");
		FileSystem moviesFs = FileSystem.get(moviesConf);

		if (moviesFs.exists(outputPath)) {
			moviesFs.delete(outputPath, true);
		}
		Job moviesJob = Job.getInstance(moviesConf);
		moviesJob.setJobName("This job does mapping of movies to cast ID");
		moviesJob.setNumReduceTasks(1);
		moviesJob.setJarByClass(GetMoviesToCastListDriver.class);
		moviesJob.setMapperClass(GetMoviesToCastListMapper.class);
		moviesJob.setReducerClass(GetMoviesToCastListReducer.class);
		FileInputFormat.addInputPath(moviesJob, inputPath);
		FileOutputFormat.setOutputPath(moviesJob, outputPath);
		moviesJob.setOutputKeyClass(Text.class);
		moviesJob.setOutputValueClass(Text.class);
		System.exit(moviesJob.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
