import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GetMoviesMetaDataDriver {
	
	public static void main(String... args) throws Exception {
		if (args.length != 2) {
			System.err.println("The usage is wrong. The correct usage is [Input path] [Output path]");
			System.exit(-1);
		}
		String inputFile = args[0];
		Path inputPath = new Path(inputFile);
		String outFile = args[1];
		Path outputPath = new Path(outFile);
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", "");
		FileSystem moviesFS = FileSystem.get(conf);
		Job moviesJob = Job.getInstance(conf);
		moviesJob.setJobName("Filter the needed columns from movie's metadata");
		moviesJob.setNumReduceTasks(0);
		moviesJob.setJarByClass(GetMoviesMetaDataDriver.class);
		moviesJob.setMapperClass(GetMoviesMetaDataMapper.class);
		if (moviesFS.exists(outputPath)) {
			moviesFS.delete(outputPath, true);
		}
		FileInputFormat.addInputPath(moviesJob, inputPath);
		FileOutputFormat.setOutputPath(moviesJob, outputPath);
		moviesJob.setOutputKeyClass(Text.class);
		moviesJob.setOutputValueClass(NullWritable.class);
		System.exit(moviesJob.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
