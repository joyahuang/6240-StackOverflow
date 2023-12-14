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

  int commentCount;
  int viewCount;
  int favCount;
  int postBodyLen;
  int tagsCount;
  int answerBodyLen;
  int answerCommentCount;
  boolean hasAcceptedAnswer;

  public PostWritable() {
  }

  public PostWritable(String postId, String acceptedAnswerId, int score, int answerCount,
      boolean isPost, int commentCount, int viewCount, int favCount,
      int postBodyLen, int tagsCount,
      boolean hasAcceptedAnswer) {
    this.postId = postId;
    this.acceptedAnswerId = acceptedAnswerId;
    this.score = score;
    this.answerCount = answerCount;
    this.acceptedScore = -1;
    this.isPost = isPost;
    this.commentCount = commentCount;
    this.viewCount = viewCount;
    this.favCount = favCount;
    this.postBodyLen = postBodyLen;
    this.tagsCount = tagsCount;
    this.answerBodyLen = -1;
    this.answerCommentCount = -1;
    this.hasAcceptedAnswer = hasAcceptedAnswer;
  }

  public PostWritable(String postId, int score, int answerBodyLen, int answerCommentCount,
      boolean isPost) {
    this.postId = postId;
    this.acceptedAnswerId = "";
    this.score = score;
    this.acceptedScore = -1;
    this.isPost = isPost;
    this.answerCount = -1;
    this.commentCount = -1;
    this.viewCount = -1;
    this.favCount = -1;
    this.postBodyLen = -1;
    this.tagsCount = -1;
    this.hasAcceptedAnswer = false;
    this.answerBodyLen = answerBodyLen;
    this.answerCommentCount = answerCommentCount;
  }

  public PostWritable(PostWritable other) {
    this.postId = other.postId;
    this.acceptedAnswerId = other.acceptedAnswerId;
    this.score = other.score;
    this.acceptedScore = other.acceptedScore;
    this.isPost = other.isPost;
    this.answerCount = other.answerCount;
    this.commentCount = other.commentCount;
    this.viewCount = other.viewCount;
    this.favCount = other.favCount;
    this.postBodyLen = other.postBodyLen;
    this.tagsCount = other.tagsCount;
    this.hasAcceptedAnswer = other.hasAcceptedAnswer;
    this.answerBodyLen = other.answerBodyLen;
    this.answerCommentCount = other.answerCommentCount;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(postId);
    out.writeUTF(acceptedAnswerId);
    out.writeInt(score);
    out.writeInt(answerCount);
    out.writeInt(acceptedScore);
    out.writeBoolean(isPost);

    out.writeInt(commentCount);
    out.writeInt(viewCount);
    out.writeInt(favCount);
    out.writeInt(postBodyLen);
    out.writeInt(tagsCount);
    out.writeInt(answerBodyLen);
    out.writeInt(answerCommentCount);
    out.writeBoolean(hasAcceptedAnswer);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    postId = dataInput.readUTF();
    acceptedAnswerId = dataInput.readUTF();
    score = dataInput.readInt();
    answerCount = dataInput.readInt();
    acceptedScore = dataInput.readInt();
    isPost = dataInput.readBoolean();

    commentCount = dataInput.readInt();
    viewCount = dataInput.readInt();
    favCount = dataInput.readInt();
    postBodyLen = dataInput.readInt();
    tagsCount = dataInput.readInt();
    answerBodyLen = dataInput.readInt();
    answerCommentCount = dataInput.readInt();
    hasAcceptedAnswer = dataInput.readBoolean();
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

  public int getAnswerBodyLen() {
    return answerBodyLen;
  }

  public void setAnswerBodyLen(int answerBodyLen) {
    this.answerBodyLen = answerBodyLen;
  }

  public int getAnswerCommentCount() {
    return answerCommentCount;
  }

  public void setAnswerCommentCount(int answerCommentCount) {
    this.answerCommentCount = answerCommentCount;
  }

  public boolean isHasAcceptedAnswer() {
    return hasAcceptedAnswer;
  }

  @Override
  public String toString() {
    return postId + ',' + score + "," + answerCount + "," + acceptedScore + "," +
        commentCount + "," + viewCount + "," + favCount + "," + postBodyLen + "," + tagsCount
        + "," + answerBodyLen + "," + answerCommentCount + "," + hasAcceptedAnswer;
  }


  public String toMyString() {
    return "PostWritable{" +
        "postId='" + postId + '\'' +
        ", acceptedAnswerId='" + acceptedAnswerId + '\'' +
        ", score=" + score +
        ", answerCount=" + answerCount +
        ", acceptedScore=" + acceptedScore +
        ", isPost=" + isPost +
        ", commentCount=" + commentCount +
        ", viewCount=" + viewCount +
        ", favCount=" + favCount +
        ", postBodyLen=" + postBodyLen +
        ", tagsCount=" + tagsCount +
        ", answerBodyLen=" + answerBodyLen +
        ", answerCommentCount=" + answerCommentCount +
        ", hasAcceptedAnswer=" + hasAcceptedAnswer +
        '}';
  }
}
