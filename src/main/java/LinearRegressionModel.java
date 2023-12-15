public class LinearRegressionModel {
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

    public double getPredictedY(double X1, double X2) {
        // formula: y_pred = b1X1 + b2X2 + b0
        return (this.b1 * X1) + (this.b2 * X2) + this.b0;
    }

    @Override
    public String toString() {
        return String.format("Linear Regression Model: predictedY = %fX1 + %fX2 + %f\n", this.b1, this.b2, this.b0);
    }
}
