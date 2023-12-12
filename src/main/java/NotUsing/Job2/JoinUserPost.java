package NotUsing.Job2;

import CSVInputFormat.CSVLineRecordReader;
import CSVInputFormat.CSVNLineInputFormat;
import NotUsing.Comparators.CompositePartitioner;
import NotUsing.Comparators.GroupComparator;
import NotUsing.Comparators.KeyComparator;
import NotUsing.Writable.CompositeKey;
import NotUsing.Writable.PostWritable;
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinUserPost {


  public static class ReplicatedJoinMapper extends
      Mapper<LongWritable, List<Text>, CompositeKey, PostWritable> {

    public ReplicatedJoinMapper()
        throws ParseException {
    }

    private FileSystem hdfs = null;
    private Map<String, Double> tagData = new HashMap<>();
    private int localMaxUserReputation = 0;
    private boolean isUser = false;

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
        tagData.put(tokens[0], Double.parseDouble(tokens[1]));
      }
    }

    public void map(LongWritable key, List<Text> values, Context context)
        throws IOException, InterruptedException {
      if (String.valueOf(values.get(0)).equals("id")) {
        return;
      }
      if (values.size() == 20) {
        // posts
        String id = String.valueOf(values.get(0));
        String userId = String.valueOf(values.get(14));
        int score = Integer.parseInt(String.valueOf(values.get(17)));
        if (userId.length() < 1) {
          return;
        }
        String tagArr = String.valueOf(values.get(18));
        String[] tags = tagArr.split("\\|");
        double tagPopularity = 0.0;
        for (String tag : tags) {
          double currPopularity = tagData.getOrDefault(tag, 0.0);
          tagPopularity = Math.max(tagPopularity, currPopularity);
        }
        String token = firstTwoDigitOfUserId(userId);
        PostWritable post = new PostWritable(id, userId, score, tagPopularity);
        CompositeKey compositeKey = new CompositeKey(token, 3);
        context.write(compositeKey, post);
      } else if (values.size() == 13) {
        // user
        isUser = true;
        String userId = String.valueOf(values.get(0));
        String token = firstTwoDigitOfUserId(userId);
        int reputation = Integer.parseInt(String.valueOf(values.get(7)));
        if (reputation > 10) {
          localMaxUserReputation = Math.max(localMaxUserReputation, reputation);
          PostWritable user = new PostWritable(userId, reputation);
          CompositeKey compositeKey = new CompositeKey(token, 2);
          context.write(compositeKey, user);
        }
      }
    }

    private String firstTwoDigitOfUserId(String userid) {
      return userid.length() >= 2 ? userid.substring(0, 2) : userid + "0";
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      if (!isUser) {
        return;
      }
      for (int i = 10; i <= 99; i++) {
        final PostWritable userWithLocalMax = new PostWritable("dummy", localMaxUserReputation);
        context.write(new CompositeKey(String.valueOf(i), 1), userWithLocalMax);
      }
    }
  }

  public static class JoinReducer extends
      Reducer<CompositeKey, PostWritable, NullWritable, PostWritable> {

    private double globalMaxUserReputation = 0.0;
    private HashMap<String, Double> userData = new HashMap<>();


    public void reduce(CompositeKey key, Iterable<PostWritable> values, Context context)
        throws IOException, InterruptedException {
      for (PostWritable value : values) {
        if (key.getRank() == 1) {
          globalMaxUserReputation = Math.max(globalMaxUserReputation, value.getReputation());
        } else if (key.getRank() == 2) {
          String userId = value.getUserId();
          userData.put(userId,
              value.getReputation() * 1.0 / globalMaxUserReputation);
        } else if (key.getRank() == 3) {
          String userId = value.getUserId();
          double reputation = userData.getOrDefault(userId, 0.0);
          value.setReputation(reputation);
          context.write(NullWritable.get(), value);
        }
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
      Job csvJob = new Job(getConf(), "Join User And Post");

      csvJob.setJarByClass(Runner.class);
      csvJob.addCacheFile(new Path(args[2]).toUri());
      csvJob.setSortComparatorClass(KeyComparator.class);
      csvJob.setPartitionerClass(CompositePartitioner.class);
      csvJob.setGroupingComparatorClass(GroupComparator.class);
      csvJob.setReducerClass(JoinReducer.class);
      csvJob.setNumReduceTasks(1);
      MultipleInputs.addInputPath(csvJob, new Path(args[0]), CSVNLineInputFormat.class,
          ReplicatedJoinMapper.class);
      MultipleInputs.addInputPath(csvJob, new Path(args[1]), CSVNLineInputFormat.class,
          ReplicatedJoinMapper.class);

      csvJob.setMapOutputKeyClass(CompositeKey.class);
      csvJob.setMapOutputValueClass(PostWritable.class);
      csvJob.setOutputKeyClass(NullWritable.class);
      csvJob.setOutputValueClass(PostWritable.class);

      FileOutputFormat.setOutputPath(csvJob, new Path(args[3]));
      csvJob.waitForCompletion(true);

      return 0;
    }

  }

}
