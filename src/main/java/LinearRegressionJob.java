import CSVInputFormat.CSVLineRecordReader;
import CSVInputFormat.CSVNLineInputFormat;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LinearRegressionJob {

  public static class RegressionMapper extends Mapper<Object, List<Text>, IntWritable, Record> {

    public void map(Object key, List<Text> values, Context context) throws IOException, InterruptedException {
        Record newRecord = new Record(values.get(1), values.get(2), values.get(3));

        context.write(new IntWritable(0), newRecord);
    }
  }

  public static class RegressionReducer extends Reducer<IntWritable, Record, NullWritable, Text> {

    public void reduce(IntWritable key, Iterable<Record> values, Context context) throws IOException, InterruptedException {
        if (key.get() != 0) {
          throw new RuntimeException("Found non-zero key in reducer.");
        }
        
        LinearRegressionExecutor linearRegressionExecutor = LinearRegressionExecutor.getInstance();
        LinearRegressionResult result = linearRegressionExecutor.execute(values);

        Text value = new Text(result.toString());
        context.write(NullWritable.get(), value);
    }
  }

  public static class Runner extends Configured implements Tool {


    public Runner() {
      this(null);
    }

    public Runner(Configuration conf) {
      super(conf);
    }


    public int run(String[] args) throws Exception {

      getConf().set(CSVLineRecordReader.FORMAT_DELIMITER, "\"");
      getConf().set(CSVLineRecordReader.FORMAT_SEPARATOR, ",");
      getConf().setInt(CSVNLineInputFormat.LINES_PER_MAP, 500000);
      getConf().setBoolean(CSVLineRecordReader.IS_ZIPFILE, false);
      Job csvJob = new Job(getConf(), "generate model");
      csvJob.setJarByClass(Runner.class);

      MultipleInputs.addInputPath(csvJob, new Path(args[0]), CSVNLineInputFormat.class,
          RegressionMapper.class);
      csvJob.setReducerClass(RegressionReducer.class);

      csvJob.setNumReduceTasks(1);
      csvJob.setMapOutputKeyClass(IntWritable.class);
      csvJob.setMapOutputValueClass(Record.class);
      csvJob.setOutputKeyClass(NullWritable.class);
      csvJob.setOutputValueClass(Text.class);

      FileOutputFormat.setOutputPath(csvJob, new Path(args[1]));
      csvJob.waitForCompletion(true);
      return 0;
    }
  }

  public static void main(String[] args) throws Exception {
    int res = -1;
    Runner importer = new Runner();

    // Let ToolRunner handle generic command-line options and run hadoop
    res = ToolRunner.run(new Configuration(), importer, args);

  }
}
