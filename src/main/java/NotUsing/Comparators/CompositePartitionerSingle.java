package NotUsing.Comparators;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CompositePartitionerSingle extends Partitioner<CompositeKey, Text> {

  @Override
  public int getPartition(CompositeKey compositeKey, Text text, int i) {
    return 0;
  }
}
