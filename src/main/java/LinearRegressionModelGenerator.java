import java.util.ArrayList;
import java.util.List;

public class LinearRegressionModelGenerator {

    private static final double TEST_SPLIT_THRESHOLD = 0.2;
    private static final LinearRegressionModelGenerator instance = new LinearRegressionModelGenerator();

    private List<Record> testData = new ArrayList<>();

    private LinearRegressionModelGenerator() {}

    public static LinearRegressionModelGenerator getInstance() {
        return instance;
    }

    // public void execute(Iterable<Record> records) {
    //     trainAndGenerateLinearRegressionModel(records);
    //     testAndEvaluateModel();
    // }

    public RegressionMetrics trainModel(Iterable<Record> records) {
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
            // System.out.println(record.toString());

            // Extract test data
            if (Math.random() <= TEST_SPLIT_THRESHOLD) {
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

        System.out.println(
            "count: " + count + "\n" + 
            "sumX1: " + sumX1 + "\n" + 
            "sumX2: " + sumX2 + "\n" + 
            "sumY: " + sumY + "\n" + 
            "sumX1Squared: " + sumX1Squared + "\n" + 
            "sumX2Squared: " + sumX2Squared + "\n" + 
            "sumX1Y: " + sumX1Y + "\n" + 
            "sumX2Y: " + sumX2Y + "\n" + 
            "sumX1X2: " + sumX1X2
        );

        // Calculates regression sums
        double regSumX1Squared = sumX1Squared - squared(sumX1) / count;
        System.out.println(regSumX1Squared);

        double regSumX2Squared = sumX2Squared - squared(sumX2) / count;
        System.out.println(regSumX2Squared);

        double regSumX1Y = sumX1Y - (sumX1 * sumY) / count;
        System.out.println(regSumX1Y);

        double regSumX2Y = sumX2Y - (sumX2 * sumY) / count;
        System.out.println(regSumX2Y);

        double regSumX1X2 = sumX1X2 - (sumX1 * sumX2) / count;
        System.out.println(regSumX1X2);

        // calculate B values
        double denominator_b1_b2 = (regSumX1Squared * regSumX2Squared) - squared(regSumX1X2);
        double b1 = ((regSumX2Squared * regSumX1Y) - (regSumX1X2 * regSumX2Y)) / denominator_b1_b2;
        double b2 = ((regSumX1Squared * regSumX2Y) - (regSumX1X2 * regSumX1Y)) / denominator_b1_b2;
        
        // calculate means for b0
        double YMean = sumY / count;
        double X1Mean = sumX1 / count;
        double X2Mean = sumX2 / count;

        double b0 = YMean - b1 * X1Mean - b2 * X2Mean;

        System.out.println(String.format("B1: %f", b1));
        System.out.println(String.format("B2: %f", b2));
        System.out.println(String.format("B0: %f", b0));

        // LinearRegressionModel model = new LinearRegressionModel(b0, b1, b2);
        // return model;

        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        // Test Model
        if (testData.isEmpty()) {
            throw new RuntimeException("Test data is empty");
        }

        double testRecordsCount = 0;
        double correctPredictionCount = 0;
        // Variables for R-Squared, MAE, RMSE
        double sumSquaredErrors = 0.0;
        double sumAbsoluteErrors = 0.0;
        double sumSquaredResiduals = 0.0;
        double meanActual = 0.0;
        List<Double> yActualList = new ArrayList<>();

        for (Record record : testData) {
            testRecordsCount++;
            double X1 = record.getX1();
            double X2 = record.getX2();
            double actualY = record.getY();

            // formula: y_pred = b1X1 + b2X2 + b0
            double predictedY = (b1 * X1) + (b2 * X2) + b0;

            System.out.println(String.format("Predicted: %f, %f, %f", predictedY, X1, X2));
            System.out.println(String.format("Actual:    %f, %f, %f", actualY, X1, X2));

            // Calculations for R-Squared, MAE, RMSE
            double error = actualY - predictedY;
            yActualList.add(actualY);
            sumSquaredErrors += Math.pow(error, 2);
            sumAbsoluteErrors += Math.abs(error);
            sumSquaredResiduals += Math.pow(actualY - predictedY, 2);
            meanActual += actualY;

            if (actualY == predictedY) {
                correctPredictionCount++;
            }
        }

        // Print accuracy
        double accuracy = correctPredictionCount / testRecordsCount;
        System.out.println("Model accuracy: " + accuracy);

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
}
