package com.founder.bigdata.compute.demo;

import com.founder.bigdata.compute.demo.config.SpringBootApplicationUtil;
import com.founder.bigdata.compute.demo.function.PeriodicStatisticResult;
import com.founder.bigdata.compute.demo.service.task.PeriodicStatistic;
import com.founder.bigdata.compute.demo.sink.TaskSink;
import com.founder.bigdata.compute.demo.source.TaskSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SpringBootStartApplication {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        if (args != null) {
            configuration.setString("args", String.join(" ", args));
        }

        SpringBootApplicationUtil.run(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);
        env.setParallelism(1);

        SingleOutputStreamOperator<PeriodicStatistic.Task> stream = env.addSource(new TaskSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PeriodicStatistic.Task>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<PeriodicStatistic.Task>) (element, recordTimestamp) -> element.timestamp)
                );

//        stream.print("input");

        DataStream dataStream = stream.keyBy(data -> data.getReportTime() + "_" + data.getTaskId())
                .process(new PeriodicStatisticResult());

        dataStream.addSink(new TaskSink());


        env.execute("PeriodicStatisticFlinkJob");

    }
}
