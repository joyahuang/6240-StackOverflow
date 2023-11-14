package TestWithCSVInputFormat;


import CSVInput.CSVLineRecordReader;
import CSVInput.CSVNLineInputFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinUserPost {


  public static class ReplicatedJoinMapper extends Mapper<LongWritable, List<Text>, Text, Text> {

    public ReplicatedJoinMapper()
        throws ParseException {
    }

    private FileSystem hdfs = null;
    private Map<String, Integer> joinData = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      try {
        this.hdfs = FileSystem.get(context.getConfiguration());
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
          for (URI cachePath : cacheFiles) {
            readFileAndPopulateJoinData(new Path(cachePath));
          }
        }
      } catch (IOException e) {
        System.err.println("Exception reading distributed cache: " + e);
      }
    }

    private void readFileAndPopulateJoinData(Path filePath) throws IOException {
      BufferedReader bufferedReader = new BufferedReader(
          new InputStreamReader(this.hdfs.open(filePath)));
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        String[] tokens = line.split("\t");
        joinData.put(tokens[0], Integer.parseInt(tokens[1]));
      }

    }

    public void map(LongWritable key, List<Text> values, Context context)
        throws IOException, InterruptedException {
      if (values.size() < 16) {
        return;
      }
      if (String.valueOf(values.get(0)).equals("id")) {
        return;
      }
      String id = String.valueOf(values.get(0));
      String owner_user_id = String.valueOf(values.get(14));

      int reputation = joinData.getOrDefault(owner_user_id, 0);
      System.out.println(owner_user_id + " " + reputation);
      context.write(new Text(id),
          new Text(String.join("_", id, owner_user_id, String.valueOf(reputation))));
    }
  }

  public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      for (Text val : values) {
        context.write(key, val);
      }
    }

  }

  public static void main(String[] args) throws Exception {
    int res = -1;
    Runner importer = new Runner();

    // Let ToolRunner handle generic command-line options and run hadoop
    res = ToolRunner.run(new Configuration(), importer, args);
  }

  public static class Runner extends Configured implements Tool {


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
      getConf().setInt(CSVNLineInputFormat.LINES_PER_MAP, 999999999);
      getConf().setBoolean(CSVLineRecordReader.IS_ZIPFILE, false);
      Job csvJob = new Job(getConf(), "csv_test_job");
      csvJob.setJarByClass(Runner.class);
      csvJob.addCacheFile(new Path(args[2]).toUri());

      MultipleInputs.addInputPath(csvJob, new Path(args[0]), CSVNLineInputFormat.class,
          ReplicatedJoinMapper.class);

      csvJob.setMapOutputValueClass(Text.class);
      csvJob.setMapOutputKeyClass(Text.class);
      csvJob.setOutputKeyClass(Text.class);
      csvJob.setOutputValueClass(Text.class);

      FileOutputFormat.setOutputPath(csvJob, new Path(args[1]));
      csvJob.waitForCompletion(true);

      return 0;
    }

  }

}
