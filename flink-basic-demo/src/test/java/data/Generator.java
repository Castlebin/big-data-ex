package data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;

import com.google.common.base.Joiner;

import cn.hutool.core.date.DatePattern;
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
            events.add(Joiner.on(',').join(time, userId, logType,
                    DateUtil.format(new Date(time), DatePattern.NORM_DATETIME_FORMAT)));
        }
        events.stream()
                .sorted()
                .forEach(System.out::println);
    }

    /**
     * 标准库
     * 在实际使用中，没有必要区重新写一次这些随机数的生成规则，可以借助一些标准库完成。如 commons-lang.
     *
     * org.apache.commons.lang3.RandomUtils 提供了如下产生指定范围的随机数方法:
     *
     * // 产生 start <= 随机数 < end 的随机整数
     * public static int nextInt(final int startInclusive, final int endExclusive);
     * // 产生 start <= 随机数 < end 的随机长整数
     * public static long nextLong(final long startInclusive, final long endExclusive);
     * // 产生 start <= 随机数 < end 的随机双精度数
     * public static double nextDouble(final double startInclusive, final double endInclusive);
     * // 产生 start <= 随机数 < end 的随机浮点数
     * public static float nextFloat(final float startInclusive, final float endInclusive);
     *
     * org.apache.commons.lang3.RandomStringUtils 提供了生成随机字符串的方法，简单介绍一下:
     *
     * // 生成指定个数的随机数字串
     * public static String randomNumeric(final int count);
     * // 生成指定个数的随机字母串
     * public static String randomAlphabetic(final int count);
     * // 生成指定个数的随机字母数字串
     * public static String randomAlphanumeric(final int count);
     *
     * stackoverflow原址：http://stackoverflow.com/questions/363681/generating-random-integers-in-a-range-with-java
     * 文章若有写得不正确或不通顺的地方，恳请你指出，谢谢。
     */

    @Test
    public void generateGiftEvents() {
        long beginTime = DateUtil.parse("2022-02-01").getTime();
        long endTime = DateUtil.parse("2022-02-06").getTime();
        List<String> events = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            long time = RandomUtils.nextLong(beginTime, endTime);
            long hostId = RandomUtils.nextLong(10, 17);
            long fanId = RandomUtils.nextLong(1000, 1030);
            long giftCount = RandomUtils.nextLong(1, 100);
            events.add(Joiner.on(',').join(time, hostId, fanId, giftCount,
                    DateUtil.format(new Date(time), DatePattern.NORM_DATETIME_FORMAT)));
        }
        events.stream()
                .sorted()
                .forEach(System.out::println);
    }

}
