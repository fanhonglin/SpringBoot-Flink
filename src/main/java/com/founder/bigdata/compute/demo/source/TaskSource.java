package com.founder.bigdata.compute.demo.source;

import cn.hutool.extra.spring.SpringUtil;
import com.alibaba.fastjson.JSON;
import com.founder.bigdata.compute.demo.common.RedisConstants;
import com.founder.bigdata.compute.demo.config.SpringBootApplicationUtil;
import com.founder.bigdata.compute.demo.service.task.PeriodicStatistic;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.StringUtils;

import java.util.concurrent.TimeUnit;

//@Component
public class TaskSource extends RichSourceFunction<PeriodicStatistic.Task> {

    private static final Logger log = LoggerFactory.getLogger(TaskSource.class);

    private Boolean running = true;

    private StringRedisTemplate stringRedisTemplate = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        SpringBootApplicationUtil.run(parameters.getString("arge", "").split(" "));
        stringRedisTemplate = SpringUtil.getBean(StringRedisTemplate.class);
    }

    @Override
    public void run(SourceContext<PeriodicStatistic.Task> ctx) {

        while (running) {
            String value = stringRedisTemplate.opsForList().rightPop(RedisConstants.QUALITY_RESULT, 1, TimeUnit.SECONDS);
            PeriodicStatistic.Task task = new PeriodicStatistic.Task();

            // 为空，那么统计空，确保timestamp有数据
            if (StringUtils.isEmpty(value)) {
                task.setTaskId("");
                task.setReportTime("");
            } else {
                task = JSON.parseObject(value, PeriodicStatistic.Task.class);
            }
            ctx.collect(task);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

}