package com.founder.bigdata.compute.demo.function;

import cn.hutool.core.date.DateUtil;
import com.founder.bigdata.compute.demo.po.StatisticsTaskPo;
import com.founder.bigdata.compute.demo.service.impl.StatisticServiceImpl;
import com.founder.bigdata.compute.demo.service.task.PeriodicStatistic;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.Objects;

import static cn.hutool.core.date.DatePattern.NORM_DATE_PATTERN;

@Component
public class PeriodicStatisticResult extends KeyedProcessFunction<String, PeriodicStatistic.Task, String> {

    private static final Logger log = LoggerFactory.getLogger(PeriodicStatisticResult.class);

    private ValueState<Long> successTotalNum;

    private ValueState<BigDecimal> totalTime;

    private ValueState<BigDecimal> qualityTotalScore;

    private ValueState<Long> eligibleTotalNum;

    private ValueState<Long> illegalTotalNum;

    private ValueState<Long> timerTsState;

    @Resource
    private StatisticServiceImpl statisticService;

    @Override
    public void open(Configuration parameters) {
        successTotalNum = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
        timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<>("timerTs", Long.class));
        totalTime = getRuntimeContext().getState(new ValueStateDescriptor<>("totalTime", BigDecimal.class));
        qualityTotalScore = getRuntimeContext().getState(new ValueStateDescriptor<>("qualityTotalScore", BigDecimal.class));
        eligibleTotalNum = getRuntimeContext().getState(new ValueStateDescriptor<>("eligibleTotalNum", Long.class));
        illegalTotalNum = getRuntimeContext().getState(new ValueStateDescriptor<>("illegalTotalNum", Long.class));
    }

    @Override
    public void processElement(PeriodicStatistic.Task task, Context ctx, Collector<String> out) throws Exception {

        if (StringUtils.isEmpty(task.getReportTime())) {
            return;
        }

        // 质检成功条数
        Long count = successTotalNum.value();
        if (count == null) {
            successTotalNum.update(1L);
        } else {
            successTotalNum.update(count + 1);
        }

        // 总时长
        BigDecimal totalTimeValue = totalTime.value();
        if (Objects.isNull(totalTimeValue)) {
            totalTime.update(new BigDecimal(0));
        } else {
            totalTime.update(task.getTotalTime().add(totalTimeValue));
        }

        // 总分数
        BigDecimal qualityTotalScoreValue = qualityTotalScore.value();
        if (Objects.isNull(qualityTotalScoreValue)) {
            qualityTotalScore.update(new BigDecimal(0));
        } else {
            qualityTotalScore.update(task.getQualityTotalScore().add(qualityTotalScoreValue));
        }

        // 违规和合格
        if (task.getEligible()) {
            Long eligibleTotalNumValue = eligibleTotalNum.value();
            if (Objects.isNull(eligibleTotalNumValue)) {
                eligibleTotalNum.update(1L);
            } else {
                eligibleTotalNum.update(eligibleTotalNumValue + 1);
            }
        } else {
            Long eligibleTotalNumValue = illegalTotalNum.value();
            if (Objects.isNull(eligibleTotalNumValue)) {
                illegalTotalNum.update(1L);
            } else {
                illegalTotalNum.update(eligibleTotalNumValue + 1);
            }
        }

        // 注册定时器
        if (timerTsState.value() == null) {
            ctx.timerService().registerEventTimeTimer(task.timestamp + 1 * 1000L);
            timerTsState.update(task.timestamp + 1 * 1000L);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 数据存储到mysql当中
//            out.collect(ctx.getCurrentKey() + " 质检成功量 : " + successTotalNum.value());
        String[] split = ctx.getCurrentKey().split("_");
        try {
            StatisticsTaskPo statisticsTaskPo = StatisticsTaskPo
                    .builder()

                    .taskId(split[1])
                    .reportTime(DateUtil.parse(split[0], NORM_DATE_PATTERN))
                    // 质检成功量
                    .successTotalNum(successTotalNum.value())
                    // 总时长
                    .totalTime(totalTime.value())
                    // 总分数
                    .qualityTotalScore(qualityTotalScore.value())
                    // 合格和违规
                    .eligibleTotalNum(eligibleTotalNum.value())
                    .illegalTotalNum(illegalTotalNum.value())

                    .build();
            statisticService.updateTaskByReportTimeAndTaskId(statisticsTaskPo);
        } catch (Exception e) {
            log.error("更新数据错误：{}", e, e.getMessage());
        }
        // 清空状态
        timerTsState.clear();
    }
}
