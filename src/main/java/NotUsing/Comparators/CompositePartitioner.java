package NotUsing.Comparators;

import org.apache.hadoop.mapreduce.Partitioner;

public class CompositePartitioner extends Partitioner<CompositeKey, PostWritable> {

  @Override
  public int getPartition(CompositeKey compositeKey, PostWritable postWritable, int i) {
    int result = (compositeKey.getToken().hashCode() & Integer.MAX_VALUE) % i;
    return result;
  }
}
