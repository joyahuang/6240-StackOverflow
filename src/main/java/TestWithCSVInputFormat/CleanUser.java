package TestWithCSVInputFormat;

import CSVInputFormat.CSVLineRecordReader;
import CSVInputFormat.CSVNLineInputFormat;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This operation is the clean the 17 user files, to remove unnecessary fields, and only store {user_id: reputation}
 * We only filter reputation > 10, because if we keep all the users, the file size is 200MB, which will be too large to keep in cache
 * By filtering reputation > 10, the file size is minimized to 50MB.
 * This filtering will not affect the ML algorithm, because if a user has <10 reputation, we can safely consider it as a zombie user and not factor in the user score.
 */
public class CleanUser {

  public static class CleanUserMapper extends Mapper<Object, List<Text>, Text, IntWritable> {

    public CleanUserMapper()
        throws ParseException {
    }


    public void map(Object key, List<Text> values, Context context)
        throws IOException, InterruptedException {

      if (values.size() != 13) {
        return;
      }
      String id = String.valueOf(values.get(0));
      if (id.equals("id")) {
        return;
      }
      Integer reputation = Integer.parseInt(String.valueOf(values.get(7)));
      if (reputation > 10) {
        context.write(new Text(id), new IntWritable(reputation));
      }

    }
  }

  public static class CleanUserReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      for (IntWritable val : values) {
        context.write(key, val);
      }

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
      getConf().setInt(CSVNLineInputFormat.LINES_PER_MAP, 10000000);
      getConf().setBoolean(CSVLineRecordReader.IS_ZIPFILE, false);
      Job csvJob = new Job(getConf(), "csv_test_job");
      csvJob.setJarByClass(Runner.class);

      MultipleInputs.addInputPath(csvJob, new Path(args[0]), CSVNLineInputFormat.class,
          CleanUserMapper.class);
      csvJob.setReducerClass(CleanUserReducer.class);

      csvJob.setNumReduceTasks(1);

      csvJob.setMapOutputKeyClass(Text.class);
      csvJob.setMapOutputValueClass(IntWritable.class);
      csvJob.setOutputKeyClass(Text.class);
      csvJob.setOutputValueClass(IntWritable.class);

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
