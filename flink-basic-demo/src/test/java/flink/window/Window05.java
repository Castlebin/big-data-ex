package flink.window;

import org.junit.jupiter.api.Test;

public class Window05 {
    /**
     * Flink 窗口计算可以有三种不同的延迟处理机制，分别是:                                <br/>
     * 1. watermarks            水位线，表示窗口延迟计算的等待时间，也就是新来的数据超过了 watermarks 的要求，窗口才会开始计算  <br/>
     *      注意：如果并发度为 N，会等到 N 个线程的 watermarks 都满足了，才会进行下一步的计算（挑出 N 个线程中最大的一个 watermarks往下发） <br/>
     * 2. allowLateness     表示在这个时间内，窗口不会关闭，会多次计算，**多次**输出结果                    <br />
     * 3. Side Output   侧输出流        延迟的数据，放入侧输出流，使用者自己决定该怎么使用测输出流       <br />
     */

    @Test
    public void stockTest2() throws Exception {

    }

}
