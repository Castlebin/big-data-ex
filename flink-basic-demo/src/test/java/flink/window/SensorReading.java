package flink.window;

// 温度传感器输出的温度事件
public class SensorReading {

    // 传感器的唯一标识
    private String key;

    // 当前温度
    private Integer value;

    // 时间戳
    private Long timestamp;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

}
