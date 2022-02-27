package flink.pojo;

public class LogEvent {
    private long time;
    private long userId;
    private String type;
    private String timeStr;

    public LogEvent() {
    }

    public LogEvent(long time, long userId, String type, String timeStr) {
        this.time = time;
        this.userId = userId;
        this.type = type;
        this.timeStr = timeStr;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTimeStr() {
        return timeStr;
    }

    public void setTimeStr(String timeStr) {
        this.timeStr = timeStr;
    }

    @Override
    public String toString() {
        return "LogEvent{" +
                "time=" + time +
                ", userId=" + userId +
                ", type='" + type + '\'' +
                ", timeStr='" + timeStr + '\'' +
                '}';
    }
}
