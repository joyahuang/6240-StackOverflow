package Comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {

  protected GroupComparator() {
    super(CompositeKey.class, true);
  }

  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {
    CompositeKey c1 = (CompositeKey) w1;
    CompositeKey c2 = (CompositeKey) w2;
    return c1.getToken().compareTo(c2.getToken());
  }
}
