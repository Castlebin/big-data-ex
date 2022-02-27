package flink.pojo;

import java.io.Serializable;

public class Iris implements Serializable {

    private long time;
    private double sepalLength;
    private double sepalWidth;
    private double petalLength;
    private double petalWidth;
    private int target;

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public double getSepalLength() {
        return sepalLength;
    }

    public void setSepalLength(double sepalLength) {
        this.sepalLength = sepalLength;
    }

    public double getSepalWidth() {
        return sepalWidth;
    }

    public void setSepalWidth(double sepalWidth) {
        this.sepalWidth = sepalWidth;
    }

    public double getPetalLength() {
        return petalLength;
    }

    public void setPetalLength(double petalLength) {
        this.petalLength = petalLength;
    }

    public double getPetalWidth() {
        return petalWidth;
    }

    public void setPetalWidth(double petalWidth) {
        this.petalWidth = petalWidth;
    }

    public int getTarget() {
        return target;
    }

    public void setTarget(int target) {
        this.target = target;
    }

    @Override
    public String toString() {
        return "Iris{" +
                "time=" + time +
                ", sepalLength=" + sepalLength +
                ", sepalWidth=" + sepalWidth +
                ", petalLength=" + petalLength +
                ", petalWidth=" + petalWidth +
                ", target=" + target +
                '}';
    }
}
