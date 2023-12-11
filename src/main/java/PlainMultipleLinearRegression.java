import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PlainMultipleLinearRegression {

    private static Integer NUM_OF_FIELDS = 3;

    public static void main(String[] args) {
        String fileName = "linearregressiontest";
        List<Record> recordList = readDataFromFile(fileName);

        if (recordList != null && recordList.size() > 0) {
            calculateCoefficients(recordList);

        } else {
            System.out.println("Error reading data from the file.");
        }
    }

    private static List<Record> readDataFromFile(String fileName) {
        List<Record> recordList = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length == NUM_OF_FIELDS) {
                    double Y = Double.parseDouble(values[0]);
                    double X1 = Double.parseDouble(values[1]);
                    double X2 = Double.parseDouble(values[2]);

                    Record newRecord = new Record(Y, X1, X2);
                    recordList.add(newRecord);
                } else {
                    System.out.println("Invalid data format: " + line);
                }
            }
        } catch (IOException | NumberFormatException e) {
            e.printStackTrace();
        }

        return recordList;
    }

    private static void calculateCoefficients(List<Record> records) {
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
            System.out.println(record.toString());
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
    }

    private static double squared(double num) {
        return Math.pow(num, 2);
    }

    static class Record {
        private double X1;
        private double X2;
        private double Y;
    
        public Record(double Y, double X1, double X2) {
            this.X1 = X1;
            this.X2 = X2;
            this.Y = Y;
        }

        protected double getX1() {
            return this.X1;
        }

        protected double getX2() {
            return this.X2;
        }

        protected double getY() {
            return this.Y;
        }

        public String toString() {
            return String.format("%f, %f, %f", this.Y, this.X1, this.X2);
        }
    }
}
