import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class RegressionMetrics implements Writable {
    private double rSquared;
    private double mae;
    private double rmse;

    public RegressionMetrics(double rSquared, double mae, double rmse) {
        this.rSquared = rSquared;
        this.mae = mae;
        this.rmse = rmse;
    }

    public double getRSquared() {
        return rSquared;
    }

    public double getMAE() {
        return mae;
    }

    public double getRMSE() {
        return rmse;
    }

        @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(rSquared);
        out.writeDouble(mae);
        out.writeDouble(rmse);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        rSquared = in.readDouble();
        mae = in.readDouble();
        rmse = in.readDouble();
    }

    @Override
    public String toString() {
        return String.format(
            "{\"R-squared\": %f, \"Mean Absolute Error\": %f, \"Root Mean Squared Error\": \"%f\"}", rSquared, mae, rmse);
    }
}