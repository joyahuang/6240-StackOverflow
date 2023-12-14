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

        int commentCount = Integer.parseInt(String.valueOf(values.get(5)));
        int viewCount = Integer.parseInt(String.valueOf(values.get(19)));
        String favCountStr = String.valueOf(values.get(8));
        int favCount = favCountStr.equals("") ? 0 : Integer.parseInt(favCountStr);

        int bodyLen = String.valueOf(values.get(2)).length();
        String tagArr = String.valueOf(values.get(18));
        String[] tags = tagArr.split("\\|");
        int tagsCount = tags.length;

        boolean hasAcceptedAnswer = true;
        if (acceptedAnswerId.length() == 0) {
          hasAcceptedAnswer = false;
          acceptedAnswerId = "dummy";
        }
        PostWritable post = new PostWritable(postId, acceptedAnswerId, score, answerCount,
            true, commentCount, viewCount, favCount, bodyLen, tagsCount, hasAcceptedAnswer);
        context.write(new Text(acceptedAnswerId), post);

      } else if (post_type_id.equals("2")) {
        // answers
        String postId = String.valueOf(values.get(0));
        int score = Integer.parseInt(String.valueOf(values.get(17)));
        int commentCount = Integer.parseInt(String.valueOf(values.get(5)));
        int bodyLen = String.valueOf(values.get(2)).length();
        PostWritable answer = new PostWritable(postId, score, bodyLen, commentCount, false);
        context.write(new Text(postId), answer);
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
        if (!value.isPost()) {
          answer = new PostWritable(value);
        } else if (value.isPost()) {
//          if (value.isHasAcceptedAnswer() == false) {
//            value.setAcceptedScore(Integer.MIN_VALUE);
//            value.setAnswerBodyLen(Integer.MIN_VALUE);
//            value.setAnswerCommentCount(Integer.MIN_VALUE);
//            context.write(NullWritable.get(), value);
//            continue;
//          } else {
          post = new PostWritable(value);
//          }
        }
      }
      if (post != null && answer != null) {
        final int acceptedScore = answer.getScore();
        final int answerBodyLen = answer.getAnswerBodyLen();
        final int answerCommentCount = answer.getAnswerCommentCount();
        post.setAcceptedScore(acceptedScore);
        post.setAnswerBodyLen(answerBodyLen);
        post.setAnswerCommentCount(answerCommentCount);
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
      csvJob.setNumReduceTasks(1);
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
