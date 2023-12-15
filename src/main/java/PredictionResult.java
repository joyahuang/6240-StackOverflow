public class PredictionResult{
    private Record predictedRecord;
    private Record actualRecord;

    public PredictionResult(Record predictedRecord, Record actualRecord) {
        this.predictedRecord = predictedRecord;
        this.actualRecord = actualRecord;
    }

    protected Record getPredictedRecord() {
        return this.predictedRecord;
    }

    protected Record getActualRecord() {
        return this.actualRecord;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        String predictedAsString = String.format("Predicted: post score: %f, answer count: %f, accepted answer score: %f\n", predictedRecord.getY(), predictedRecord.getX1(), predictedRecord.getX2());
        String actualAsString = String.format("Actual:    post score: %f, answer count: %f, accepted answer score: %f\n", actualRecord.getY(), actualRecord.getX1(), actualRecord.getX2());
        String recordSeparator ="----------------------------------------------------------------------------------------\n";

        sb.append(predictedAsString);
        sb.append(actualAsString);
        sb.append(recordSeparator);

        return sb.toString();
    }
}
