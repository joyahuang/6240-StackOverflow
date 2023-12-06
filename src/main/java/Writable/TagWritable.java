package Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class TagWritable implements WritableComparable<TagWritable> {
  private String tagName;
  private Integer popularity;

  public TagWritable() {
  }

  public TagWritable(String tagName, Integer popularity) {
    this.tagName = tagName;
    this.popularity = popularity;
  }

  @Override
  public int compareTo(TagWritable o) {
    return 0;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(tagName);
    dataOutput.writeInt(popularity);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    tagName = dataInput.readUTF();
    popularity = dataInput.readInt();
  }

  public String getTagName() {
    return tagName;
  }

  public Integer getPopularity() {
    return popularity;
  }
}
