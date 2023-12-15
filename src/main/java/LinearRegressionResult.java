import java.util.List;

public class LinearRegressionResult {
    private LinearRegressionModel model;
    private RegressionMetrics metrics;
    private List<PredictionResult> predictionResults;

    public LinearRegressionResult(LinearRegressionModel model, RegressionMetrics metrics, List<PredictionResult> predictionResults) {
        this.model = model;
        this.metrics = metrics;
        this.predictionResults = predictionResults;
    }

    public LinearRegressionResult() {}

    protected LinearRegressionModel getModel() {
        return this.model;
    }

    protected RegressionMetrics getMetrics() {
        return this.metrics;
    }

    protected List<PredictionResult> getPredictionResults() {
        return this.predictionResults;
    }

    protected void setModel(LinearRegressionModel model) {
        this.model = model;
    }

    protected void setMetrics(RegressionMetrics metrics) {
        this.metrics = metrics;
    }

    protected void setPredictionResults(List<PredictionResult> predictionResults) {
        this.predictionResults = predictionResults;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (PredictionResult result : this.predictionResults) {
            sb.append(result.toString());
        }

        String modelAsString = this.model.toString();
        String metricsAsString = this.metrics.toString();

        sb.append(modelAsString);
        sb.append(metricsAsString);

        return sb.toString();
    }
}
