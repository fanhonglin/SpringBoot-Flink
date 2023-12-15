package com.founder.bigdata.compute.demo;

import cn.hutool.extra.spring.SpringUtil;
import com.alibaba.fastjson.JSON;
import com.founder.bigdata.compute.demo.common.RedisConstants;
import com.founder.bigdata.compute.demo.service.impl.StatisticServiceImpl;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.StringUtils;

import java.util.Calendar;
import java.util.Objects;
import java.util.concurrent.TimeUnit;


@SpringBootApplication
@MapperScan
@Slf4j
public class QualitySubmitApplication {
    public static void main(String[] args) throws Exception {

        SpringApplication.run(QualitySubmitApplication.class, args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<QualityTaskSubmit> stream = env.addSource(new QualityTaskSubmitSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<QualityTaskSubmit>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<QualityTaskSubmit>) (element, recordTimestamp) -> element.timestamp));

//        DataStream<QualityTaskSubmit> stream = env.addSource(new QualityTaskSubmitSource());

        stream.print("input");

        stream.keyBy(data -> data.getReportTime() + "_" + data.getTasKId()).process(new PeriodicStatisticResult()).print();

        env.execute();
    }

    public static class PeriodicStatisticResult extends KeyedProcessFunction<String, QualityTaskSubmit, String> {

        private ValueState<Long> qualityTotalNumber;

        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) {
            qualityTotalNumber = getRuntimeContext().getState(new ValueStateDescriptor<>("quality-Total-Number", Long.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<>("quality-Total-TimerTs", Long.class));
        }

        @Override
        public void processElement(QualityTaskSubmit qualityTaskSubmit, Context ctx, Collector<String> out) throws Exception {

            Long count = qualityTotalNumber.value();
            if (Objects.isNull(count)) {
                qualityTotalNumber.update(1L);
            } else {
                qualityTotalNumber.update(count + 1);
            }

            if (Objects.isNull(timerTsState.value())) {
                ctx.timerService().registerEventTimeTimer(qualityTaskSubmit.timestamp + 10 * 1000L);
                timerTsState.update(qualityTaskSubmit.timestamp + 10 * 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            String currentKey = ctx.getCurrentKey();
            System.out.println("key: " + currentKey + " ， 质检量:" + qualityTotalNumber.value());
            String[] split = currentKey.split("_");
            try {
                SpringUtil.getBean(StatisticServiceImpl.class).updateByReportTimeAndTaskId(split[0], split[1], qualityTotalNumber.value());
            } catch (Exception e) {
                log.error("更新数据错误：{}", e, e.getMessage());
            }

            // 清空状态
            timerTsState.clear();
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class QualityTaskSubmit {

        private String tasKId;
        private String reportTime;

        @Builder.Default
        public Long timestamp = Calendar.getInstance().getTimeInMillis();
    }

    public static class QualityTaskSubmitSource implements SourceFunction<QualityTaskSubmit> {
        private Boolean running = true;

        @Override
        public void run(SourceContext<QualityTaskSubmit> ctx) {

            StringRedisTemplate stringRedisTemplate = SpringUtil.getBean(StringRedisTemplate.class);

            while (running) {

                String value = stringRedisTemplate.opsForList().rightPop(RedisConstants.TASK_TOTAL_NUM, 1, TimeUnit.SECONDS);
                if (StringUtils.isEmpty(value)) {
                    continue;
                }
                QualityTaskSubmit qualityTaskSubmit = JSON.parseObject(value, QualityTaskSubmit.class);
                ctx.collect(qualityTaskSubmit);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
