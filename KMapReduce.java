import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.collect.Iterables;

public class KMapReduce {

	public static class Map extends Mapper<Object, Text, Text, Text> {
		private Text centroid = new Text();

		private final List<Point> list = new ArrayList<>();
		
		@SuppressWarnings("deprecation")
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path centroids = new Path(conf.get("centroid.path"));
			FileSystem fs = FileSystem.get(conf);

			try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids, context.getConfiguration())) {
				IntWritable value = new IntWritable();
				Text key = new Text();
				while (reader.next(key, value)) {
					list.add(Point.parsePoint(key.toString()));
				}
			}
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());

			while (itr.hasMoreTokens()) {
				Point point = Point.parsePoint(itr.nextToken());
				Double min = Double.MAX_VALUE;
				for (Point c : list) {
					if (Point.getDistance(point, c) < min) {
						min = Point.getDistance(point, c);
						centroid = new Text(Point.printPoint(c));
					}
				}
				context.write(centroid, new Text(Point.printPoint(point)));
			}
		}
	}

	public static class Reduce extends Reducer<Object, Text, Text, Text> {
		private final List<Point> listPoint = new ArrayList<>();

		public static enum Counter {
			CONVERGED
		}

		@Override
		public void reduce(Object key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Double x = 0.0;
			Double y = 0.0;
			List<Text> temp = new ArrayList<Text>();
			Double distance = 0.0;
			String points = "";
			for (Text val : values) {
				x += (Point.parsePoint(String.valueOf(val)).getX());
				y += (Point.parsePoint(String.valueOf(val)).getY());

				points += val.toString();
				temp.add(val);
			}
			int size = Iterables.size(temp);
			Point newCentroid = new Point(x / size, y / size);
			listPoint.add(newCentroid);
			System.out.println(size);
			
			for(Text t: temp){
				distance += Point.getDistance(newCentroid, Point.parsePoint(t.toString()));
			}
			context.write(new Text(Point.printPoint(newCentroid) + ": \n"), new Text(distance.toString()));
			System.out.println(Point.printPoint(newCentroid) + ": " + points);

			if (Point.getDistance(Point.parsePoint(key.toString()), newCentroid) > 0) {
				context.getCounter(Counter.CONVERGED).increment(1);
			}
			System.out.println(Point.getDistance(Point.parsePoint(key.toString()), newCentroid));
		}

		@SuppressWarnings("deprecation")
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path outPath = new Path(conf.get("centroid.path"));
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(outPath)) {
				fs.delete(outPath, true);
			}
			try (SequenceFile.Writer writer = SequenceFile.createWriter(fs, context.getConfiguration(), outPath,
					Text.class, IntWritable.class)) {
				final IntWritable value = new IntWritable(0);
				for (Point p : listPoint) {
					writer.append(new Text(Point.printPoint(p)), value);
				}
			}
		}
	}

	@SuppressWarnings("deprecation")
	public static void writeExampleCenters(Configuration conf, Path center, FileSystem fs) throws IOException {
		try (SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, center, Text.class,
				IntWritable.class)) {
			final IntWritable value = new IntWritable(0);
			centerWriter.append(new Text(Point.printPoint(new Point(4.0, 5.0))), value);
			centerWriter.append(new Text(Point.printPoint(new Point(100.0, 100.0))), value);
			centerWriter.append(new Text(Point.printPoint(new Point(200.0, 200.0))), value);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Path inputPath = new Path("/home/zakarum/workspace/KMapReduce/src/data.txt");
		Path outputPath = new Path("/home/zakarum/workspace/KMapReduce/src/output");
		Path center = new Path("/home/zakarum/workspace/KMapReduce/src/cen.seq");
		conf.set("centroid.path", center.toString());

		Job job = Job.getInstance(conf);

		// Path inputPath = new Path(args[0]);
		// Path outputPath = new Path(args[1]);
		job.setJobName("Kmeans");
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setJarByClass(KMapReduce.class);

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath);
		}

		if (fs.exists(center)) {
			fs.delete(center);
		}

		writeExampleCenters(conf, center, fs);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
		int iteration = 0;
		long counter = job.getCounters().findCounter(Reduce.Counter.CONVERGED).getValue();
		while (counter > 0) {
			conf = new Configuration();
			conf.set("centroid.path", center.toString());
			job = Job.getInstance(conf);

			job.setJobName("loop");
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setJarByClass(KMapReduce.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			if (fs.exists(outputPath)) {
				fs.delete(outputPath);
			}
			FileInputFormat.addInputPath(job, inputPath);
			FileOutputFormat.setOutputPath(job, outputPath);

			job.waitForCompletion(true);
			counter = job.getCounters().findCounter(Reduce.Counter.CONVERGED).getValue();
			iteration++;
		}

		System.out.println(iteration);
	}
}
