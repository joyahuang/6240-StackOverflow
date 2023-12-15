import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Record implements Writable{
    private double X1;
    private double X2;
    private double Y;

    public Record(Text Y, Text X1, Text X2) {
        this.X1 = Double.parseDouble(X1.toString());
        this.X2 = Double.parseDouble(X2.toString());
        this.Y = Double.parseDouble(Y.toString());
    }

    public Record(double Y, double X1, double X2) {
        this.X1 = X1;
        this.X2 = X2;
        this.Y = Y;
    }

    public Record() {}

    protected double getX1() {
        return this.X1;
    }

    protected double getX2() {
        return this.X2;
    }

    protected double getY() {
        return this.Y;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(Y);
        out.writeDouble(X1);
        out.writeDouble(X2);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        Y = in.readDouble();
        X1 = in.readDouble();
        X2 = in.readDouble();
    }

    public String toString() {
        return String.format("%f, %f, %f", this.Y, this.X1, this.X2);
    }

    public Record copy() {
        return new Record(this.Y, this.X1, this.X2);
    }
}
