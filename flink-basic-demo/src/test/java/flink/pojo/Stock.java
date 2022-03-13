package flink.pojo;

import java.io.Serializable;

public class Stock implements Serializable {

    // 股票代码
    private String code;

    // 股票价格
    private float price;

    // 时间
    private long time;

    public Stock() {
    }

    public Stock(String code, float price, long time) {
        this.code = code;
        this.price = price;
        this.time = time;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "Stock{" +
                "code='" + code + '\'' +
                ", price=" + price +
                ", time=" + time +
                '}';
    }

}
