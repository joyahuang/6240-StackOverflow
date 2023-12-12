package JoinPostsAndAnswers;

import CSVInputFormat.CSVLineRecordReader;
import CSVInputFormat.CSVNLineInputFormat;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

public class JoinAnswerPost {

  public static class ReplicatedJoinMapper extends
      Mapper<LongWritable, List<Text>, Text, PostWritable> {

    public ReplicatedJoinMapper()
        throws ParseException {
    }


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    public void map(LongWritable key, List<Text> values, Context context)
        throws IOException, InterruptedException {
      if (values.size() != 20 || String.valueOf(values.get(0)).equals("id")) {
        return;
      }
      String post_type_id = String.valueOf(values.get(16));
      if (post_type_id.equals("1")) {
        // posts
        String postId = String.valueOf(values.get(0));
        String acceptedAnswerId = String.valueOf(values.get(3));
        int score = Integer.parseInt(String.valueOf(values.get(17)));
        int answerCount = Integer.parseInt(String.valueOf(values.get(4)));
        if (acceptedAnswerId.length() > 0) {
          PostWritable post = new PostWritable(postId, acceptedAnswerId, score, answerCount,
              0, true);
          context.write(new Text(acceptedAnswerId), post);
        }
      } else if (post_type_id.equals("2")) {
        // answers
        String postId = String.valueOf(values.get(0));
        int score = Integer.parseInt(String.valueOf(values.get(17)));
        PostWritable post = new PostWritable(postId, "0", score, 0, 0, false);
        context.write(new Text(postId), post);
      }
    }

  }

  public static class JoinReducer extends
      Reducer<Text, PostWritable, NullWritable, PostWritable> {

    public void reduce(Text key, Iterable<PostWritable> values,
        Context context)
        throws IOException, InterruptedException {
      PostWritable post = null;
      PostWritable answer = null;
      for (PostWritable value : values) {
        if (value.isPost()) {
          post = value;
        } else {
          answer = value;
        }
      }
      if (post != null && answer != null) {
        post.setAcceptedScore(answer.getScore());
        context.write(NullWritable.get(), post);
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
      Job csvJob = new Job(getConf(), "Join Answers And Post");

      csvJob.setJarByClass(Runner.class);
      csvJob.setReducerClass(JoinReducer.class);
      csvJob.setNumReduceTasks(10);
      MultipleInputs.addInputPath(csvJob, new Path(args[0]), CSVNLineInputFormat.class,
          ReplicatedJoinMapper.class);
      MultipleInputs.addInputPath(csvJob, new Path(args[1]), CSVNLineInputFormat.class,
          ReplicatedJoinMapper.class);

      csvJob.setMapOutputKeyClass(Text.class);
      csvJob.setMapOutputValueClass(PostWritable.class);
      csvJob.setOutputKeyClass(NullWritable.class);
      csvJob.setOutputValueClass(PostWritable.class);

      FileOutputFormat.setOutputPath(csvJob, new Path(args[2]));
      csvJob.waitForCompletion(true);
      return 0;
    }

  }
}
