import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class LinearRegressionModel implements Writable {
    private double b0;
    private double b1;
    private double b2;
    
    public LinearRegressionModel(double b0, double b1, double b2) {
        this.b0 = b0;
        this.b1 = b1;
        this.b2 = b2;
    }

    public LinearRegressionModel() {}

    public double getB0() {
        return this.b0;
    }

    public double getB1() {
        return this.b1;
    }

    public double getB2() {
        return this.b2;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(b0);
        out.writeDouble(b1);
        out.writeDouble(b2);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        b0 = in.readDouble();
        b1 = in.readDouble();
        b2 = in.readDouble();
    }

    @Override
    public String toString() {
        return String.format(
            "{\"b0\": %f, \"b1\": %f, \"b2\": \"%f\"}", b0, b1, b2);
    }
}
