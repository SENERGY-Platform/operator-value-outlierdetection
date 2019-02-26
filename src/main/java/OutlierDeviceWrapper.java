public class OutlierDeviceWrapper {
    private long time;
    private double value;
    private Welford welford;

    public OutlierDeviceWrapper(){
        welford = new Welford();
    }

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


    public Welford getWelford() {
        return welford;
    }

    public void setWelford(Welford welford) {
        this.welford = welford;
    }
}
