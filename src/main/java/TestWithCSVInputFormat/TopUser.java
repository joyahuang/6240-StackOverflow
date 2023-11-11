package TestWithCSVInputFormat;

import CSVInput.CSVLineRecordReader;
import CSVInput.CSVNLineInputFormat;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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

public class TopUser {

  private static int topN = 10;

  public static class UserMapper extends Mapper<Object, List<Text>, Text, IntWritable> {

    private TreeMap<Integer, String> tmap; // by default ascending order

    public UserMapper()
        throws ParseException {
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      tmap = new TreeMap<Integer, String>();
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
      tmap.put(reputation, id);
      if (tmap.size() > topN) {
        tmap.remove(tmap.firstKey());
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      for (Map.Entry<Integer, String> entry : tmap.entrySet()) {
        Integer reputation = entry.getKey();
        String id = entry.getValue();
        context.write(new Text(id), new IntWritable(reputation));
      }
    }
  }

  public static class TopUsersReducer extends Reducer<Text, IntWritable, IntWritable, Text> {

    private TreeMap<Integer, String> tmap;

    @Override
    public void setup(Context context) {
      tmap = new TreeMap<Integer, String>();
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      String id = key.toString();
      for (IntWritable val : values) {
        int rep = val.get();
        tmap.put(rep, id);
        if (tmap.size() > topN) {
          tmap.remove(tmap.firstKey());
        }
      }

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      for (Map.Entry<Integer, String> entry : tmap.entrySet()) {
        Integer reputation = entry.getKey();
        String id = entry.getValue();
        context.write(new IntWritable(reputation), new Text(id));
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
      csvJob.setMapOutputKeyClass(Text.class);
      csvJob.setMapOutputValueClass(IntWritable.class);
      MultipleInputs.addInputPath(csvJob, new Path(args[0]), CSVNLineInputFormat.class,
          UserMapper.class);
      csvJob.setReducerClass(TopUsersReducer.class);
      csvJob.setOutputKeyClass(IntWritable.class);
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
