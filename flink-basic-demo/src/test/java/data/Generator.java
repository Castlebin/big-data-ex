package data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.google.common.base.Joiner;

import cn.hutool.core.date.DateUtil;

public class Generator {

    /**
     * 生成一份随机的用户登录登出数据
     * */
    @Test
    public void generateLoginLogOutData() {
        Random random = new Random();
        List<Integer> userIds = IntStream.rangeClosed(10, 17).boxed().collect(Collectors.toList());
        List<String> logTypes = Arrays.asList("login", "logout");
        long beginTime = DateUtil.parse("2022-02-01").getTime();
        long endTime = DateUtil.parse("2022-02-06").getTime();
        List<String> events = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            long time = random.nextInt((int) (endTime - beginTime)) + beginTime;
            long userId = userIds.get(random.nextInt(userIds.size()));
            String logType = logTypes.get(random.nextInt(logTypes.size()));
            events.add(Joiner.on(',').join(time, userId, logType));
        }
        events.stream()
                .sorted()
                .forEach(System.out::println);
    }

}
