package TestWithCSVInputFormat;


import CSVInputFormat.CSVLineRecordReader;
import CSVInputFormat.CSVNLineInputFormat;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class StackOverflow {

  private static final Logger logger = Logger.getLogger(StackOverflow.class.getName());

  public static class PostMapper extends Mapper<LongWritable, List<Text>, Text, Text> {

    public PostMapper()
        throws ParseException {
    }

    public void map(LongWritable key, List<Text> values, Context context)
        throws IOException, InterruptedException {
      logger.info("PostMapper");
      logger.info("key=" + key);
      int i = 0;
      for (Text val : values) {
        logger.info("key=" + key + " val[" + (i++) + "] = " + val);
      }
      List<String> stringList = values.stream().map(Text::toString).collect(Collectors.toList());
      String joinedString = String.join("@", stringList);
      context.write(new Text("test"), new Text(joinedString));
//      String id = String.valueOf(values.get(0));
//      String owner_user_id = String.valueOf(values.get(16));
//      context.write(new Text(owner_user_id),
//          new Text(String.join("_", "post", id, owner_user_id)));

    }
  }


  public static class UserMapper extends Mapper<Object, List<Text>, Text, Text> {


    public UserMapper()
        throws ParseException {
    }

    public void map(Object key, List<Text> values, Context context)
        throws IOException, InterruptedException {
      String id = String.valueOf(values.get(0));
      String reputation = String.valueOf(values.get(7));
      context.write(new Text(id), new Text(String.join("_", "user", id, reputation)));
    }
  }


  public static class JoinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      for (Text text : values) {
        context.write(new IntWritable(1), text);
      }
//      String post_id = "";
//      String user_reputation = "";
//
//      for (Text value : values) {
//        String parts[] = value.toString().split("_");
//        if (parts[0].equals("post")) {
//          post_id = parts[1];
//        } else if (parts[0].equals("user")) {
//          user_reputation = parts[1];
//        }
//      }
//
//      if (!post_id.isEmpty() && !user_reputation.isEmpty()) {
//        context.write(new IntWritable(Integer.parseInt(post_id)),
//            new Text(String.join(",", post_id, key.toString(), user_reputation)));
//      }
    }

  }

  public static void main(String[] args) throws Exception {
    int res = -1;
    logger.info("Initializing CSV Test Runner");
    Runner importer = new Runner();

    // Let ToolRunner handle generic command-line options and run hadoop
    res = ToolRunner.run(new Configuration(), importer, args);
    logger.info("ToolRunner finished running hadoop with res code " + res);
  }

  public static class Runner extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(Runner.class.getName());

    private static final String INPUT_PATH_PREFIX = "./src/test/resources/bull.csv";
    // private static final String INPUT_PATH_PREFIX = "/tmp/importer_tests/";

    public Runner() {
      this(null);
    }

    public Runner(Configuration conf) {
      super(conf);
    }


    public int run(String[] args) throws Exception {

      getConf().set(CSVLineRecordReader.FORMAT_DELIMITER, "\"");
      getConf().set(CSVLineRecordReader.FORMAT_SEPARATOR, ",");
      getConf().setInt(CSVNLineInputFormat.LINES_PER_MAP, 40000);
      getConf().setBoolean(CSVLineRecordReader.IS_ZIPFILE, false);
      Job csvJob = new Job(getConf(), "csv_test_job");
      csvJob.setJarByClass(Runner.class);
      csvJob.setNumReduceTasks(0);

      MultipleInputs.addInputPath(csvJob, new Path(args[0]), CSVNLineInputFormat.class,
          PostMapper.class);
      MultipleInputs.addInputPath(csvJob, new Path(args[1]), CSVNLineInputFormat.class,
          UserMapper.class);

      csvJob.setOutputKeyClass(IntWritable.class);
      csvJob.setOutputValueClass(Text.class);
      logger.info("Process will begin");
      FileOutputFormat.setOutputPath(csvJob, new Path(args[2]));
      csvJob.waitForCompletion(true);

      logger.info("Process ended");

      return 0;
    }

  }

}
