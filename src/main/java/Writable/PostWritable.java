package Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class PostWritable implements Writable {

  String postId;
  String userId;
  int score;
  Double reputation;
  Double popularity;

  public PostWritable() {
  }

  public PostWritable(String postId, String userId, int score,
      Double popularity) {
    this.postId = postId;
    this.userId = userId;
    this.score = score;
    this.reputation = -1.0;
    this.popularity = popularity;
  }

  public PostWritable(String userId, int reputation) {
    this.userId = userId;
    this.postId = " ";
    this.score = -1;
    this.reputation = reputation * 1.0;
    this.popularity = -1.0;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(postId);
    out.writeUTF(userId);
    out.writeInt(score);
    out.writeDouble(reputation);
    out.writeDouble(popularity);

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    postId = dataInput.readUTF();
    userId = dataInput.readUTF();
    score = dataInput.readInt();
    reputation = dataInput.readDouble();
    popularity = dataInput.readDouble();
  }


  public String getUserId() {
    return userId;
  }


  public void setReputation(Double reputation) {
    this.reputation = reputation;
  }

  public Double getReputation() {
    return reputation;
  }

  @Override
  public String toString() {
    return postId + ',' + score + "," + reputation +"," + popularity;
  }
}
