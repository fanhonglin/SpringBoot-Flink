package com.founder.bigdata.compute.demo.controller;


import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.founder.bigdata.compute.demo.service.task.PeriodicStatistic;
import com.founder.bigdata.compute.demo.common.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.founder.bigdata.compute.demo.common.RedisConstants.TASK_TOTAL_NUM;

@RestController
public class Controller {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @GetMapping("sendQualityTotalNum")
    public void sendQualityTotalNum() {

        // 统计数据
        Map<String, Object> sendTotalStatisticPipMap = new HashMap<>();
        sendTotalStatisticPipMap.put("reportTime", DateUtil.format(new Date(), DatePattern.NORM_DATE_PATTERN));
        sendTotalStatisticPipMap.put("taskId", "123");

        // 质检
        stringRedisTemplate.opsForList().leftPush(TASK_TOTAL_NUM, JSON.toJSONString(sendTotalStatisticPipMap));
    }

    @GetMapping("sendQualityTask")
    public void sendQualityTask() {

        // 统计数据
        PeriodicStatistic.Task task = PeriodicStatistic.Task.builder()
                .taskId("123")
                .reportTime(DateUtil.format(new Date(), DatePattern.NORM_DATE_PATTERN))
                .qualityTotalScore(new BigDecimal(10))
                .totalTime(new BigDecimal(2))
                .eligible(true)
                .build();
        // 质检
        stringRedisTemplate.opsForList().leftPush(RedisConstants.QUALITY_RESULT, JSON.toJSONString(task));
    }
}
