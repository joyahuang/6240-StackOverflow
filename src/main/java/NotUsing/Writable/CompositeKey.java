package NotUsing.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey> {

  private String token;
  private Integer rank;

  public CompositeKey(String token, Integer rank) {
    this.token = token;
    this.rank = rank;
  }

  public CompositeKey() {
  }

  @Override
  public int compareTo(CompositeKey o) {
    int result = this.token.compareTo(o.token);
    if (result == 0) {
      result = this.rank.compareTo(o.rank);
    }
    return result;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(token);
    dataOutput.writeInt(rank);

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    token = dataInput.readUTF();
    rank = dataInput.readInt();
  }

  public String getToken() {
    return token;
  }

  public int getRank() {
    return rank;
  }

  @Override
  public String toString() {
    return "CompositeKey{" +
        "token='" + token + '\'' +
        ", rank=" + rank +
        '}';
  }
}

