package Job1;

import CSVInputFormat.CSVLineRecordReader;
import CSVInputFormat.CSVNLineInputFormat;
import Comparators.CompositePartitionerSingle;
import Comparators.GroupComparator;
import Comparators.KeyComparator;
import Writable.CompositeKey;
import Writable.TagWritable;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CleanTag {

  public static class CleanTagMapper extends Mapper<Object, List<Text>, CompositeKey, TagWritable> {


    private Integer localMaxTagPopularity = null;

    @Override
    protected void setup(Context context) {
      localMaxTagPopularity = Integer.MIN_VALUE;
    }

    public void map(Object key, List<Text> values, Context context)
        throws IOException, InterruptedException {
      if (values.size() != 5 || String.valueOf(values.get(0)).equals("id")) {
        return;
      }
      String tagname = String.valueOf(values.get(1));
      Integer popularity = Integer.parseInt(String.valueOf(values.get(2)));
      localMaxTagPopularity = Math.max(localMaxTagPopularity, popularity);
      if (popularity > 5) {
        context.write(new CompositeKey("tag", 2), new TagWritable(tagname, popularity));
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      context.write(new CompositeKey("tag", 1), new TagWritable("dummy", localMaxTagPopularity));
    }
  }


  public static class CleanTagReducer extends
      Reducer<CompositeKey, TagWritable, Text, DoubleWritable> {

    private Integer globalMaxTagPopularity = Integer.MIN_VALUE;

    public void reduce(CompositeKey key, Iterable<TagWritable> values, Context context
    )
        throws IOException, InterruptedException {
      if (key.getRank() == 1) {
        for (TagWritable tag : values) {
          globalMaxTagPopularity = Math.max(globalMaxTagPopularity, tag.getPopularity());
        }
      } else {
        for (TagWritable tag : values) {
          context.write(new Text(tag.getTagName()),
              new DoubleWritable(tag.getPopularity() * 1.0 / globalMaxTagPopularity));
        }
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
      Job csvJob = new Job(getConf(), "clean tags");
      csvJob.setJarByClass(Runner.class);

      MultipleInputs.addInputPath(csvJob, new Path(args[0]), CSVNLineInputFormat.class,
          CleanTagMapper.class);
      csvJob.setReducerClass(CleanTagReducer.class);

      csvJob.setNumReduceTasks(1);
      csvJob.setPartitionerClass(CompositePartitionerSingle.class);
      csvJob.setGroupingComparatorClass(GroupComparator.class);
      csvJob.setSortComparatorClass(KeyComparator.class);

      csvJob.setMapOutputKeyClass(CompositeKey.class);
      csvJob.setMapOutputValueClass(TagWritable.class);
      csvJob.setOutputKeyClass(Text.class);
      csvJob.setOutputValueClass(DoubleWritable.class);

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
