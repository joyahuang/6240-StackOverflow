package JoinPostsAndAnswers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class PostWritable implements Writable {

  String postId;
  String acceptedAnswerId;
  int score;
  int answerCount;
  int acceptedScore;
  boolean isPost;

  public PostWritable() {
  }

  public PostWritable(String postId, String acceptedAnswerId, int score, int answerCount,
      int acceptedScore, boolean isPost) {
    this.postId = postId;
    this.acceptedAnswerId = acceptedAnswerId;
    this.score = score;
    this.answerCount = answerCount;
    this.acceptedScore = acceptedScore;
    this.isPost = isPost;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(postId);
    out.writeUTF(acceptedAnswerId);
    out.writeInt(score);
    out.writeInt(answerCount);
    out.writeInt(acceptedScore);
    out.writeBoolean(isPost);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    postId = dataInput.readUTF();
    acceptedAnswerId = dataInput.readUTF();
    score = dataInput.readInt();
    answerCount = dataInput.readInt();
    acceptedScore = dataInput.readInt();
    isPost = dataInput.readBoolean();
  }

  public boolean isPost() {
    return isPost;
  }

  public void setAcceptedScore(int acceptedScore) {
    this.acceptedScore = acceptedScore;
  }

  public int getScore() {
    return score;
  }

  @Override
  public String toString() {
    return postId + ',' + score +
        "," + answerCount +
        ", " + acceptedScore;
  }
}
