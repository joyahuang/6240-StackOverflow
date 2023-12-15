import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class LinearRegressionExecutor {

    private static final double TEST_SPLIT_THRESHOLD = 0.2;
    private static final LinearRegressionExecutor instance = new LinearRegressionExecutor();

    private List<Record> testData = new ArrayList<>();

    private LinearRegressionExecutor() {}

    public static LinearRegressionExecutor getInstance() {
        return instance;
    }

    public LinearRegressionResult execute(Iterable<Record> records) {
        LinearRegressionResult result = new LinearRegressionResult();

        double count = 0;
        double sumX1 = 0;
        double sumX2 = 0;
        double sumY = 0;
        double sumX1Squared = 0;
        double sumX2Squared = 0;
        double sumX1Y = 0;
        double sumX2Y = 0;
        double sumX1X2 = 0;

        for (Record record : records) {

            // Extract test data
            if (random() <= TEST_SPLIT_THRESHOLD) {
                testData.add(record.copy());
                continue;
            }
            
            // pull apart X1, X2, Y
            double X1 = record.getX1();
            double X2 = record.getX2();
            double Y = record.getY();

            count++;
            sumX1 += X1;
            sumX2 += X2;
            sumY += Y;
            sumX1Squared += squared(X1);
            sumX2Squared += squared(X2);
            sumX1Y += X1 * Y;
            sumX2Y += X2 * Y;
            sumX1X2 += X1 * X2;
        }

        LinearRegressionModel model = generateModel(count, sumX1, sumX2, sumY, sumX1Y, sumX2Y, sumX1X2, sumX1Squared, sumX2Squared);
        result.setModel(model);

        runTestsAndGenerateMetrics(result, testData);

        return result;
    }

    private LinearRegressionModel generateModel(double count, double sumX1, double sumX2, double sumY, double sumX1Y, double sumX2Y, double sumX1X2, double sumX1Squared, double sumX2Squared) {
        // Calculates regression sums
        double regSumX1Squared = sumX1Squared - squared(sumX1) / count;
        double regSumX2Squared = sumX2Squared - squared(sumX2) / count;
        double regSumX1Y = sumX1Y - (sumX1 * sumY) / count;
        double regSumX2Y = sumX2Y - (sumX2 * sumY) / count;
        double regSumX1X2 = sumX1X2 - (sumX1 * sumX2) / count;

        // calculate B values
        double denominator_b1_b2 = (regSumX1Squared * regSumX2Squared) - squared(regSumX1X2);
        double b1 = ((regSumX2Squared * regSumX1Y) - (regSumX1X2 * regSumX2Y)) / denominator_b1_b2;
        double b2 = ((regSumX1Squared * regSumX2Y) - (regSumX1X2 * regSumX1Y)) / denominator_b1_b2;
        
        // calculate means for b0
        double YMean = sumY / count;
        double X1Mean = sumX1 / count;
        double X2Mean = sumX2 / count;

        double b0 = YMean - b1 * X1Mean - b2 * X2Mean;

        return new LinearRegressionModel(b0, b1, b2);
    }

    private LinearRegressionResult runTestsAndGenerateMetrics(LinearRegressionResult result, List<Record> testData) {
        if (testData.isEmpty()) {
            throw new RuntimeException("Test data is empty");
        }

        // Variables for R-Squared, MAE, RMSE
        double sumSquaredErrors = 0.0;
        double sumAbsoluteErrors = 0.0;
        double sumSquaredResiduals = 0.0;
        double meanActual = 0.0;
        List<Double> yActualList = new ArrayList<>();
        List<PredictionResult> samplePredictionResultsList = new ArrayList<>();

        for (Record record : testData) {
            double X1 = record.getX1();
            double X2 = record.getX2();
            double actualY = record.getY();

            double predictedY = result.getModel().getPredictedY(X1, X2);

            // Print out a small subset of predicted records
            if (random() <= 0.08) {
                Record predictedRecord = new Record(predictedY, X1, X2);
                PredictionResult predictionResult = new PredictionResult(predictedRecord, record);
                samplePredictionResultsList.add(predictionResult);
            }

            // Calculations for R-Squared, MAE, RMSE
            double error = actualY - predictedY;
            yActualList.add(actualY);
            sumSquaredErrors += Math.pow(error, 2);
            sumAbsoluteErrors += Math.abs(error);
            sumSquaredResiduals += Math.pow(actualY - predictedY, 2);
            meanActual += actualY;
        }

        result.setPredictionResults(samplePredictionResultsList);

        RegressionMetrics metrics = calculatePerformanceMetrics(yActualList, sumSquaredResiduals, sumAbsoluteErrors, sumSquaredErrors, meanActual);
        result.setMetrics(metrics);

        return result;
    }

    private RegressionMetrics calculatePerformanceMetrics(List<Double> yActualList, double sumSquaredResiduals, double sumAbsoluteErrors, double sumSquaredErrors, double meanActual) {
         // Calculate R-Squared, MAE, RMSE
        meanActual /= yActualList.size();

        double ssTot = 0.0;
        for (double actual : yActualList) {
            ssTot += Math.pow(actual - meanActual, 2);
        }

        double rSquared = 1 - (sumSquaredResiduals / ssTot);
        double mae = sumAbsoluteErrors / yActualList.size();
        double rmse = Math.sqrt(sumSquaredErrors / yActualList.size());

        return new RegressionMetrics(rSquared, mae, rmse);
    }

    private static double squared(double num) {
        return Math.pow(num, 2);
    }

    private static double random() {
        Random random = new Random(System.nanoTime());
        return random.nextDouble();
    }
}
