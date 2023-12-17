package com.founder.bigdata.compute.demo.source;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.founder.bigdata.compute.demo.common.RedisConstants;
import com.founder.bigdata.compute.demo.service.task.PeriodicStatistic;
import com.founder.bigdata.compute.demo.util.SpringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.math.BigDecimal;
import java.util.Date;
import java.util.concurrent.TimeUnit;

//@Component
public class TaskSource extends RichSourceFunction<PeriodicStatistic.Task> {

    private static final Logger log = LoggerFactory.getLogger(TaskSource.class);

    private Boolean running = true;

//    private StringRedisTemplate stringRedisTemplate;

    @Override
    public void open(Configuration parameters) {
//        stringRedisTemplate = SpringUtils.getBean(StringRedisTemplate.class);
//        log.info("stringRedisTemplate------------------------------------------:{}", stringRedisTemplate);
    }

    @Override
    public void run(SourceContext<PeriodicStatistic.Task> ctx) {

        while (running) {

//            String value = stringRedisTemplate.opsForList().rightPop(RedisConstants.QUALITY_RESULT, 1, TimeUnit.SECONDS);
//            PeriodicStatistic.Task task = new PeriodicStatistic.Task();
//
//            // 为空，那么统计空，确保timestamp有数据
//            if (StringUtils.isEmpty(value)) {
//                task.setTaskId("");
//                task.setReportTime("");
//            } else {
//                task = JSON.parseObject(value, PeriodicStatistic.Task.class);
//            }

            PeriodicStatistic.Task task = PeriodicStatistic.Task.builder()
                    .taskId("123")
                    .reportTime(DateUtil.format(new Date(), DatePattern.NORM_DATE_PATTERN))
                    .qualityTotalScore(new BigDecimal(10))
                    .totalTime(new BigDecimal(2))
                    .eligible(true)
                    .build();

            ctx.collect(task);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

}