package com.founder.bigdata.compute.demo.service.task;


import cn.hutool.extra.spring.SpringUtil;
import com.founder.bigdata.compute.demo.function.PeriodicStatisticResult;
import com.founder.bigdata.compute.demo.source.TaskSource;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Calendar;

@Component
public class PeriodicStatistic {

    private static final Logger log = LoggerFactory.getLogger(PeriodicStatistic.class);

    @PostConstruct
    public void start() {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);
        env.setParallelism(1);

        SingleOutputStreamOperator<Task> stream = env.addSource(new TaskSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Task>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Task>) (element, recordTimestamp) -> element.timestamp)
                );

        stream.print("input");

        stream.keyBy(data -> data.getReportTime() + "_" + data.getTaskId())
                .process(SpringUtil.getBean(PeriodicStatisticResult.class))
                .print();

        new Thread(() -> {
            try {
                env.execute("PeriodicStatisticFlinkJob");
            } catch (Exception e) {
                log.error(e.toString(), e);
            }
        }).start();
    }


    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Task implements Serializable {

        private String taskId;

        private String reportTime;

        private BigDecimal totalTime;

        private BigDecimal qualityTotalScore;

        // true: 合格， false：违规
        private Boolean eligible;

        // 时间戳
        @Builder.Default
        public Long timestamp = Calendar.getInstance().getTimeInMillis();
    }


}


