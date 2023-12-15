package com.founder.bigdata.compute.demo.controller;


import com.alibaba.fastjson.JSON;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

import static com.founder.bigdata.compute.demo.common.RedisConstants.TASK_TOTAL_NUM;

@RestController
public class Controller {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @GetMapping
    public void send() {

        // 统计数据
        Map<String, Object> sendTotalStatisticPipMap = new HashMap<>();
        sendTotalStatisticPipMap.put("reportTime", "2023-12-15");
        sendTotalStatisticPipMap.put("tasKId", "123");

        // 质检
        stringRedisTemplate.opsForList().leftPush(TASK_TOTAL_NUM, JSON.toJSONString(sendTotalStatisticPipMap));
    }
}
