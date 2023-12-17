public class LinearRegressionMetrics {
    private double rSquared;
    private double mae;
    private double rmse;

    public LinearRegressionMetrics(double rSquared, double mae, double rmse) {
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
    public String toString() {
        return String.format(
            "R-squared: %f, Mean Absolute Error (MAE): %f, Root Mean Squared Error (RMSE): %f\n", rSquared, mae, rmse);
    }
}