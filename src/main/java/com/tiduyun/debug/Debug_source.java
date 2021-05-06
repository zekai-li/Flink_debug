package com.tiduyun.debug;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author hzj
 * @date 2021/5/7 1:21
 */
public class Debug_source implements SourceFunction<String> {
        public Long num = 0l;
        public Boolean isCancel =false;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (!isCancel){
        ctx.collect(""+num);
        Thread.sleep(100);
        num++;
        }
    }
    @Override
    public void cancel() {
        System.out.println("cancel");
        isCancel=true;
    }

}
