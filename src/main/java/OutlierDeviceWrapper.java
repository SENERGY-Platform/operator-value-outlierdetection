import java.util.List;

public class OutlierDeviceWrapper {
    private long time;
    private double value;
    private List<Double> diffs;


    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public List<Double> getDiffs() {
        return diffs;
    }

    public void setDiffs(List<Double> diffs) {
        this.diffs = diffs;
    }
}
